import { type FormEvent, useState } from "react";
import type { CommandRequest } from "../api/client";
import type { CommandRun } from "../useCommandRun";

// Ejemplos de eda_retail.synthetic.sample.EXAMPLES; el backend valida la lista blanca.
export const EXAMPLES = ["delay", "loss", "out_of_stock", "injection"] as const;

// Qué provoca cada ejemplo (synthetic/sample.py): loss y out_of_stock son pérdidas, pero P-1004 no
// tiene stock y su reserva falla.
export const EXAMPLE_INFO: Record<(typeof EXAMPLES)[number], string> = {
  delay: "Retraso de 24 h en P-1001: el plan avisa al cliente.",
  loss: "Pérdida de P-1002: reclamar, reservar stock y reenviar.",
  out_of_stock: "Pérdida de P-1004, sin stock: la reserva falla y se revisa el plan.",
  injection:
    "Retraso de P-1003 con una orden inyectada (reembolsar 1000 EUR): se trata como dato y los reembolsos tienen un tope.",
};
const MAX_TEXT = 2000;
const DEFAULT_TIMEOUT = 180;

function formatSeconds(seconds: number): string {
  return `${seconds.toLocaleString("es-ES", { minimumFractionDigits: 1, maximumFractionDigits: 1 })} s`;
}

function activeKey(request: CommandRequest | null): string | null {
  if (!request) return null;
  if (request.command === "send") return request.example ? "example" : request.text ? "text" : "send";
  return request.command;
}

interface Props {
  run: CommandRun;
  // `send --text` deshabilitado salvo que el backend lo admita (WEB_FREE_TEXT).
  freeText?: boolean;
  onRun: (request: CommandRequest) => void;
  onSendLine: (line: string) => void;
}

// Presentacional: tarjetas de los comandos de scripts/notice.sh, salida y entrada interactiva.
// «Preparación» (seed, sin eventos) va aparte de «Avisos» (los comandos que inician el flujo).
export function CommandMenu({ run, freeText = false, onRun, onSendLine }: Props) {
  const [text, setText] = useState("");
  const [timeout, setTimeout] = useState(String(DEFAULT_TIMEOUT));
  const [line, setLine] = useState("");
  const [last, setLast] = useState<CommandRequest | null>(null);
  const busy = run.status === "running";
  const timeoutValue = Number(timeout);
  const timeoutOk = Number.isInteger(timeoutValue) && timeoutValue >= 1 && timeoutValue <= 600;
  const active = busy ? activeKey(last) : null;

  const launch = (request: CommandRequest) => {
    setLast(request);
    onRun(request);
  };

  const submitLine = (event: FormEvent) => {
    event.preventDefault();
    onSendLine(line);
    setLine("");
  };

  const card = (key: string) => `card${active === key ? " card--active" : ""}`;

  return (
    <aside className="commands" aria-labelledby="commands-title">
      <h2 id="commands-title" className="section-label">
        Comandos
      </h2>
      <p className="commands__script">scripts/notice.sh</p>

      <section className="commands__group" aria-labelledby="prep-title">
        <h3 id="prep-title" className="group-label">
          Preparación
        </h3>
        <button type="button" className={card("seed")} disabled={busy} onClick={() => launch({ command: "seed" })}>
          <code>seed</code>{" "}
          <span>Carga los pedidos P-1001 a P-1005 y su stock.</span>{" "}
          <span className="card__note">No genera eventos: son datos de referencia, no hechos del dominio.</span>
        </button>
      </section>

      <section className="commands__group" aria-labelledby="notices-title">
        <h3 id="notices-title" className="group-label">
          Avisos
        </h3>
        <button type="button" className={card("send")} disabled={busy} onClick={() => launch({ command: "send" })}>
          <code>send</code>{" "}
          <span>Aviso de ejemplo: retraso de P-1001.</span>
        </button>

        <div className={card("example")}>
          <code>send --example</code>
          <span>Elige el tipo de aviso y se envía al pulsarlo.</span>
          <div className="chips">
            {EXAMPLES.map((example) => (
              <button
                key={example}
                type="button"
                className={`chip${active === "example" && last?.command === "send" && last.example === example ? " chip--active" : ""}`}
                title={EXAMPLE_INFO[example]}
                disabled={busy}
                onClick={() => launch({ command: "send", example })}
              >
                {example}
              </button>
            ))}
          </div>
          <ul className="examples" aria-label="Qué provoca cada ejemplo">
            {EXAMPLES.map((example) => (
              <li key={example}>
                <strong>{example}</strong>: {EXAMPLE_INFO[example]}
              </li>
            ))}
          </ul>
        </div>

        {freeText && (
          <div className={card("text")}>
            <code>send --text</code>
            <label htmlFor="free-text">Texto libre del aviso</label>
            <textarea
              id="free-text"
              rows={3}
              maxLength={MAX_TEXT}
              value={text}
              onChange={(event) => setText(event.target.value)}
            />
            <button
              type="button"
              className="primary"
              disabled={busy || text.trim() === ""}
              onClick={() => launch({ command: "send", text })}
            >
              Enviar aviso
            </button>
          </div>
        )}

        <div className={card("interactive")}>
          <div className="inline">
            <button
              type="button"
              className="link"
              disabled={busy || !timeoutOk}
              onClick={() => launch({ command: "interactive", timeout: timeoutValue })}
            >
              <code>interactive --timeout</code>
            </button>
            <input
              type="number"
              aria-label="timeout"
              min={1}
              max={600}
              value={timeout}
              onChange={(event) => setTimeout(event.target.value)}
            />
          </div>
          <span>Menú interactivo que sigue cada incidencia hasta su cierre.</span>
        </div>
      </section>

      <h2 id="output-title" className="section-label">
        Salida
      </h2>
      <pre className={`output output--${run.status}`} role="log" aria-labelledby="output-title">
        {run.status === "idle" && "Sin ejecuciones todavía. Elige un comando."}
        {run.status === "error" && run.message}
        {(run.status === "running" || run.status === "done") && (
          <>
            {`$ ${run.argv.join(" ")}\n`}
            {run.text}
            {run.status === "done" && (
              <span className="output__exit">{`terminó con código ${run.exitCode} en ${formatSeconds(run.seconds ?? 0)}`}</span>
            )}
          </>
        )}
      </pre>
      {run.status === "running" && run.interactive && (
        <form className="input-line" onSubmit={submitLine}>
          <input
            type="text"
            aria-label="Entrada"
            maxLength={200}
            value={line}
            onChange={(event) => setLine(event.target.value)}
            autoFocus
          />
          <button type="submit">Enviar</button>
        </form>
      )}
    </aside>
  );
}
