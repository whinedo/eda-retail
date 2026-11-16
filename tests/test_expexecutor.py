"""Expexecutor (spec expexecutor): ejecución de experimentos e informe de resultados.

Sin PostgreSQL, Kafka ni LLM: `eda-experiment` se sustituye por un runner falso que escribe lo mismo que
él (la carpeta de la ejecución con `report.json` y `report.md`, y su salida en el log).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from eda_retail.experiments.cli import DEFAULT_TEMPERATURE, experiment_kwargs, parse_args
from eda_retail.expexecutor import cli, config
from eda_retail.expexecutor.results import RESULTS, write_results

E1_METRICS = {"runs": [
    {"deliveries": 2, "idempotency": "full", "duplicates_total": 0},
    {"deliveries": 2, "idempotency": "no_effect_keys", "duplicates_total": 3},
    {"deliveries": 2, "idempotency": "none", "duplicates_total": 5},
]}


def report_data(experiment: str = "E1", *, seed: int = 42, metrics=None, hypothesis=None) -> dict:
    return {
        "experiment": experiment, "seed": seed, "config": {"incidents": 4}, "model": "oracle",
        "metrics": E1_METRICS if metrics is None else metrics,
        "hypothesis": {"H1": True} if hypothesis is None else hypothesis,
        "runs": [], "prompts_version": "0123456789abcdef",
    }


def make_run(out: Path, name: str = "e1-20261116T100000", *, data: dict | None = None,
             log: str | None = "H1: cumplida\n") -> Path:
    folder = out / name
    folder.mkdir(parents=True)
    (folder / "report.json").write_text(json.dumps(data or report_data()))
    (folder / "report.md").write_text("# E1\n")
    if log is not None:
        (folder / "experiment.log").write_text(log)
    return folder


class FakeRunner:
    """Hace lo que `eda-experiment`: escribe `<out>/<exp>-<fecha>/report.json` y su salida en el log."""

    def __init__(self, *, code: int = 0, write_report: bool = True, metrics=None, hypothesis=None,
                 output: str = "H1: cumplida\n"):
        self.calls: list[list[str]] = []
        self.code, self.write_report, self.output = code, write_report, output
        self.metrics, self.hypothesis = metrics, hypothesis

    def __call__(self, argv: list[str], log) -> int:
        self.calls.append(argv)
        log.write(self.output)
        if self.write_report:
            exp, out = argv[0], Path(argv[argv.index("--out") + 1])
            seed = int(argv[argv.index("--seed") + 1])
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
            folder = out / f"{exp}-{stamp}"
            folder.mkdir(parents=True)
            data = report_data(exp.upper(), seed=seed, metrics=self.metrics, hypothesis=self.hypothesis)
            (folder / "report.json").write_text(json.dumps(data))
            (folder / "report.md").write_text(f"# {exp.upper()}\n")
        return self.code

    def option(self, name: str) -> str | None:
        argv = self.calls[-1]
        return argv[argv.index(name) + 1] if name in argv else None


def run(tmp_path: Path, *args: str, runner: FakeRunner | None = None, config_text: str | None = None):
    """`eda-expexecutor run` en `tmp_path`, con su propio fichero de configuración."""
    cfg = tmp_path / "config.py"
    cfg.write_text(config_text if config_text is not None else "EXPERIMENTS = {}\n")
    runner = runner or FakeRunner()
    code = cli.main(["run", *args, "--out", str(tmp_path / "results"), "--config", str(cfg)], runner=runner)
    return code, runner


def run_folders(tmp_path: Path) -> list[Path]:
    base = tmp_path / "results"
    return sorted(p for p in base.iterdir() if p.is_dir()) if base.exists() else []


def section(text: str, title: str) -> str:
    return text.split(f"## {title}\n", 1)[1].split("\n## ", 1)[0]


# --- Agente determinista ------------------------------------------------------------------------


def test_same_report_for_the_same_run(tmp_path, monkeypatch):
    """Mismo informe para la misma ejecución"""
    from eda_retail.llm import ollama_client

    def no_llm(*args, **kwargs):
        raise AssertionError("el expexecutor no llama a ningún LLM")

    monkeypatch.setattr(ollama_client.OllamaClient, "__init__", no_llm)
    folder = make_run(tmp_path)
    first = write_results(folder).read_bytes()
    second = write_results(folder).read_bytes()
    assert first == second


# --- CLI ----------------------------------------------------------------------------------------


def test_cli_help_lists_the_subcommands(capsys):
    """Ayuda del CLI"""
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert all(name in out for name in ("run", "report", "delete"))


def test_unknown_subcommand_fails_in_spanish(capsys):
    """Subcomando desconocido"""
    with pytest.raises(SystemExit) as exc:
        cli.main(["publish"])
    assert exc.value.code != 0
    assert "opción no válida" in capsys.readouterr().err


# --- Ejecución ----------------------------------------------------------------------------------


def test_full_run(tmp_path):
    """Ejecución completa"""
    code, runner = run(tmp_path, "e1", "--seed", "42")
    assert code == 0
    [folder] = run_folders(tmp_path)
    assert {p.name for p in folder.iterdir()} == {"report.json", "report.md", "experiment.log", RESULTS}
    assert json.loads((folder / "report.json").read_text())["seed"] == 42
    assert runner.calls[0][0] == "e1" and runner.option("--seed") == "42"
    assert "H1: cumplida" in (folder / "experiment.log").read_text()


def test_unmet_hypothesis_is_not_a_failed_run(tmp_path):
    """Hipótesis no cumplida: eda-experiment termina con 1 y escribe su informe; no es una ejecución fallida"""
    runner = FakeRunner(code=1, metrics={"open": 1, "repeated_steps": 0}, hypothesis={"H2": False})
    code, _ = run(tmp_path, "e2", runner=runner)
    assert code == 1
    [folder] = run_folders(tmp_path)
    text = (folder / RESULTS).read_text()
    assert "ejecución fallida" not in text
    assert "no coincide" in section(text, "Resultado")


def test_failed_run(tmp_path):
    """Experimento que falla"""
    output = "".join(f"línea {n}\n" for n in range(1, 31)) + "Traceback (most recent call last):\nboom\n"
    code, _ = run(tmp_path, "e2", runner=FakeRunner(code=2, write_report=False, output=output))
    assert code == 2
    [folder] = run_folders(tmp_path)
    assert folder.name.startswith("e2-")
    log = (folder / "experiment.log").read_text()
    assert output in log
    text = (folder / RESULTS).read_text()
    assert "ejecución fallida" in section(text, "Resultado")
    evidence = section(text, "Evidencias")
    assert "boom" in evidence and "Traceback" in evidence
    assert "`línea 1`" not in evidence  # solo las últimas líneas del log


def test_unknown_experiment(tmp_path):
    """Experimento desconocido"""
    code, runner = run(tmp_path, "e99")
    assert code != 0
    assert runner.calls == []
    assert run_folders(tmp_path) == []


# --- Fichero de configuración -------------------------------------------------------------------


def test_parameters_from_the_config_file(tmp_path):
    """Parámetros desde el fichero"""
    code, runner = run(tmp_path, "e5", runner=FakeRunner(hypothesis={"H5": True},
                                                          metrics={"accuracy_mean": 0.9, "threshold": 0.8}),
                       config_text='EXPERIMENTS = {"e5": {"llm": "oracle", "repetitions": 3}}\n')
    assert code == 0
    assert runner.option("--llm") == "oracle" and runner.option("--repetitions") == "3"
    [folder] = run_folders(tmp_path)
    evidence = section((folder / RESULTS).read_text(), "Evidencias")
    assert "llm = oracle (fichero de configuración)" in evidence
    assert "repetitions = 3 (fichero de configuración)" in evidence


def test_command_line_wins_over_the_file(tmp_path):
    """La línea de comandos gana al fichero"""
    code, runner = run(tmp_path, "e1", "--seed", "42", config_text='EXPERIMENTS = {"e1": {"seed": 7}}\n')
    assert code == 0 and runner.option("--seed") == "42"
    [folder] = run_folders(tmp_path)
    assert "seed = 42 (línea de comandos)" in section((folder / RESULTS).read_text(), "Evidencias")


def test_experiment_without_entry_uses_the_defaults(tmp_path):
    """Experimento sin entrada en el fichero"""
    code, runner = run(tmp_path, "e4", runner=FakeRunner(hypothesis={"H4": True}, metrics={"views": {}}),
                       config_text='EXPERIMENTS = {"e5": {"llm": "oracle"}}\n')
    assert code == 0
    assert runner.option("--seed") == "42" and runner.option("--incidents") == "200"
    assert runner.option("--llm") is None
    [folder] = run_folders(tmp_path)
    assert "seed = 42 (valor por defecto)" in section((folder / RESULTS).read_text(), "Evidencias")


def test_alternative_config_file(tmp_path):
    """Fichero de configuración alternativo"""
    other = tmp_path / "mi-config.py"
    other.write_text('EXPERIMENTS = {"e2": {"incidents": 5}}\n')
    runner = FakeRunner(hypothesis={"H2": True}, metrics={"open": 0, "repeated_steps": 0})
    code = cli.main(["run", "e2", "--out", str(tmp_path / "results"), "--config", str(other)], runner=runner)
    assert code == 0 and runner.option("--incidents") == "5"


def test_parameter_not_valid_for_the_experiment(tmp_path, capsys):
    """Parámetro no válido para el experimento"""
    code, runner = run(tmp_path, "e1", config_text='EXPERIMENTS = {"e1": {"threshold": 0.9}}\n')
    assert code != 0
    err = capsys.readouterr().err
    assert "threshold" in err and "e1" in err
    assert runner.calls == [] and run_folders(tmp_path) == []


def test_value_of_the_wrong_type(tmp_path):
    """Valor de tipo incorrecto"""
    code, runner = run(tmp_path, "e5", config_text='EXPERIMENTS = {"e5": {"repetitions": "tres"}}\n')
    assert code != 0
    assert runner.calls == [] and run_folders(tmp_path) == []


def test_unknown_experiment_or_parameter_in_the_file_is_rejected():
    """El fichero se valida antes de ejecutar: experimento o parámetro desconocidos"""
    with pytest.raises(config.ConfigError):
        config.validate({"e99": {}})
    with pytest.raises(config.ConfigError):
        config.validate({"e1": {"speed": 2}})


def test_defaults_are_the_ones_of_eda_experiment():
    """Experimento sin entrada en el fichero: los valores por defecto son los de eda-experiment"""
    for exp in ("e1", "e5", "e6"):
        args = parse_args([exp])
        kwargs = experiment_kwargs(args)
        applied = {a.name: a.value for a in config.resolve(exp, cli={}, file={})}
        assert applied["seed"] == args.seed and applied["incidents"] == kwargs["incidents"]
        if exp in ("e5", "e6"):
            assert applied["repetitions"] == kwargs["repetitions"]
            assert applied["temperature"] == DEFAULT_TEMPERATURE


# --- Generación del informe de una ejecución existente ------------------------------------------


def test_regenerate_a_report(tmp_path):
    """Regenerar un informe"""
    folder = make_run(tmp_path)
    runner = FakeRunner()
    assert cli.main(["report", str(folder)], runner=runner) == 0
    assert (folder / RESULTS).exists()
    assert runner.calls == []


def test_previous_run_without_log(tmp_path):
    """Ejecución anterior sin log"""
    folder = make_run(tmp_path, log=None)
    assert cli.main(["report", str(folder)]) == 0
    evidence = section((folder / RESULTS).read_text(), "Evidencias")
    assert "`report.json`" in evidence
    assert "no hay log de la ejecución" in evidence.lower()


def test_folder_without_experiment_report(tmp_path):
    """Carpeta sin informe del experimento"""
    folder = tmp_path / "e1-20261116T100000"
    folder.mkdir()
    assert cli.main(["report", str(folder)]) != 0
    assert not (folder / RESULTS).exists()


# --- Formato y evidencias -----------------------------------------------------------------------


def test_report_sections(tmp_path):
    """Secciones del informe"""
    text = write_results(make_run(tmp_path)).read_text()
    lines = text.splitlines()
    assert lines[0] == "# Resultados de E1"
    headings = [line for line in lines if line.startswith("## ")]
    assert headings == ["## Propósito", "## Resultado", "## Evidencias"]
    result = section(text, "Resultado")
    assert "H1" in result and "Esperado" in result and "Obtenido" in result
    assert "coinciden" in result.lower() and "no coincide" not in result


def test_unmet_hypothesis(tmp_path):
    """Hipótesis no cumplida"""
    data = report_data("E2", metrics={"open": 1, "repeated_steps": 0}, hypothesis={"H2": False})
    text = write_results(make_run(tmp_path, "e2-20261116T100000", data=data)).read_text()
    result = section(text, "Resultado")
    assert "no coincide" in result
    assert "metrics.open" in result.split("Difiere en", 1)[1]


def test_evidence_cites_its_origin(tmp_path):
    """Evidencias con su origen"""
    evidence = section(write_results(make_run(tmp_path)).read_text(), "Evidencias")
    items = [line for line in evidence.splitlines() if line.startswith("- ")]
    assert items
    assert all("report.json" in item or "experiment.log" in item for item in items)
    assert "`report.json` › `metrics.runs`" in evidence
    assert "`report.json` › `seed`" in evidence


def test_log_errors_as_evidence(tmp_path):
    """Errores del log como evidencia"""
    log = "inicio\nERROR eda_retail.harness: el broker no responde\nH1: cumplida\n"
    evidence = section(write_results(make_run(tmp_path, log=log)).read_text(), "Evidencias")
    assert "`experiment.log`, línea 2" in evidence
    assert "el broker no responde" in evidence


def test_librdkafka_warnings_as_evidence(tmp_path):
    """Errores del log como evidencia: los avisos de librdkafka (%0-%4) cuentan como avisos"""
    log = (
        "inicio\n"
        "%4|1794868414.229|GETPID|rdkafka#producer-7| [thrd:main]: Coordinator load in progress\n"
        "%6|1794868414.300|INFO|rdkafka#producer-7| [thrd:main]: connected\n"
        "H1: cumplida\n"
    )
    evidence = section(write_results(make_run(tmp_path, log=log)).read_text(), "Evidencias")
    assert "`experiment.log`, línea 2" in evidence
    assert "`experiment.log`, línea 3" not in evidence


# --- Borrado ------------------------------------------------------------------------------------


def test_delete_one_report(tmp_path):
    """Borrar un informe"""
    first = make_run(tmp_path, "e1-20261116T100000")
    second = make_run(tmp_path, "e2-20261116T100001")
    for f in (first, second):
        (f / RESULTS).write_text("x")
    assert cli.main(["delete", str(first)]) == 0
    assert not (first / RESULTS).exists() and (second / RESULTS).exists()
    assert all((first / name).exists() for name in ("report.json", "report.md", "experiment.log"))


def test_delete_all_experiment_data(tmp_path):
    """Borrar todos los datos de los experimentos"""
    out = tmp_path / "experiments" / "results"
    for name in ("e1-20261116T100000", "e2-20261116T100001", "e5-20261116T100002"):
        (make_run(out, name) / RESULTS).write_text("x")
    cfg = tmp_path / "experiments" / "config.py"
    cfg.write_text("EXPERIMENTS = {}\n")
    assert cli.main(["delete", "--all", "--out", str(out)]) == 0
    assert [p for p in out.iterdir() if p.is_dir()] == []
    assert cfg.exists()


def test_delete_a_report_that_does_not_exist(tmp_path):
    """Borrar un informe que no existe"""
    folder = make_run(tmp_path)
    assert cli.main(["delete", str(folder)]) != 0
    assert {p.name for p in folder.iterdir()} == {"report.json", "report.md", "experiment.log"}


def test_delete_without_target(tmp_path):
    """Borrado sin destino"""
    folder = make_run(tmp_path)
    (folder / RESULTS).write_text("x")
    assert cli.main(["delete", "--out", str(tmp_path)]) != 0
    assert (folder / RESULTS).exists()
