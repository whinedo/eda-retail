import type { PocVersion } from "../useConfig";
import type { HealthState } from "../useHealth";

const VERSION_LABEL: Record<PocVersion, string> = { 1: "Versión 1", 2: "Versión 2 (MCP + A2A)" };

function Pill({ ok, children }: { ok: boolean | null; children: string }) {
  const tone = ok === null ? "pending" : ok ? "ok" : "down";
  return (
    <li className={`pill pill--${tone}`}>
      <span className="pill__dot" aria-hidden="true" />
      {children}
    </li>
  );
}

// Presentacional: estado de la infraestructura y de los procesos.
export function Header({
  health,
  jaegerUrl,
  onReset,
  resetDisabled = false,
  analyticsOpen = false,
  onAnalytics,
  pocVersion = null,
}: {
  health: HealthState;
  jaegerUrl: string;
  // Versión de la PoC con la que corre el backend; sin ella (cargando) no hay insignia.
  pocVersion?: PocVersion | null;
  onReset?: () => void;
  resetDisabled?: boolean;
  analyticsOpen?: boolean;
  onAnalytics?: () => void;
}) {
  return (
    <header className="header">
      <div className="header__title">
        <strong>EDA Retail</strong>
        <span>Incidencias logísticas · PoC académica</span>
      </div>
      {pocVersion !== null && (
        <span className={`header__version header__version--v${pocVersion}`}>{VERSION_LABEL[pocVersion]}</span>
      )}
      {onAnalytics && (
        <a
          className="header__link header__link--analytics"
          href="#/analytics"
          aria-current={analyticsOpen ? "page" : undefined}
          onClick={(e) => {
            e.preventDefault();
            onAnalytics();
          }}
        >
          Analítica
        </a>
      )}
      {onReset && (
        <button
          type="button"
          className="header__reset"
          onClick={onReset}
          disabled={resetDisabled}
          title={resetDisabled ? "Espera a que termine el comando" : "Vacía la vista y vuelve a empezar"}
        >
          Resetear simulación
        </button>
      )}
      <a className="header__link" href={jaegerUrl} target="_blank" rel="noopener noreferrer">
        Jaeger
      </a>
      <ul className="header__status" aria-label="Estado de la infraestructura">
        {health.kind === "loading" && <Pill ok={null}>Comprobando…</Pill>}
        {health.kind === "offline" && <Pill ok={false}>Backend: sin conexión</Pill>}
        {health.kind === "ok" && (
          <>
            <Pill ok={health.health.infra.kafka === "healthy"}>{`Kafka: ${health.health.infra.kafka}`}</Pill>
            <Pill ok={health.health.infra.postgres === "healthy"}>
              {`PostgreSQL: ${health.health.infra.postgres}`}
            </Pill>
            <Pill ok={health.health.infra.ollama === "healthy"}>{`Ollama: ${health.health.infra.ollama}`}</Pill>
            <Pill ok={health.health.processes_up === health.health.processes_total}>
              {`Procesos: ${health.health.processes_up} de ${health.health.processes_total}`}
            </Pill>
          </>
        )}
      </ul>
    </header>
  );
}
