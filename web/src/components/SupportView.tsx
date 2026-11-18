import { useState } from "react";
import { type SupportAction, type SupportArgs, type SupportCard, type SupportStatus, argsKey } from "../useSupport";

// Secciones del diagnóstico de `eda-support status`, en el orden de la spec.
const SECTIONS: { key: string; label: string }[] = [
  { key: "processes", label: "Procesos" },
  { key: "topic", label: "Topic (marcas de agua por partición)" },
  { key: "groups", label: "Grupos de Kafka" },
  { key: "read_side", label: "Lag del lado de lectura" },
  { key: "outbox", label: "Outbox por componente" },
  { key: "dead_letters", label: "Dead letters" },
  { key: "failed_jobs", label: "Trabajos fallidos" },
  { key: "parked_notices", label: "Avisos aparcados" },
];

export interface SupportViewProps {
  enabled: boolean;
  components: string[];
  views: string[];
  status: SupportStatus;
  cards: Partial<Record<SupportAction, SupportCard>>;
  busy: boolean;
  onRefresh: () => void;
  onSimulate: (args: SupportArgs) => void;
  onExecute: (args: SupportArgs) => void;
  onBack: () => void;
}

function value(raw: unknown): string {
  if (raw === null || raw === undefined) return "sin datos";
  if (typeof raw === "object") {
    return Object.entries(raw as Record<string, unknown>)
      .map(([k, v]) => `${k}: ${typeof v === "object" && v !== null ? JSON.stringify(v) : String(v)}`)
      .join(" · ");
  }
  return String(raw);
}

function Section({ label, data }: { label: string; data: unknown }) {
  const rows = data !== null && typeof data === "object" ? Object.entries(data as Record<string, unknown>) : null;
  return (
    <section className="operations__section support__section" aria-label={label}>
      <h3>{label}</h3>
      {rows === null ? (
        <p className="operations__note">{value(data)}</p>
      ) : rows.length === 0 ? (
        <p className="operations__note">ninguno</p>
      ) : (
        <table className="events__table">
          <tbody>
            {rows.map(([key, item]) => (
              <tr key={key}>
                <td>{key}</td>
                <td>{value(item)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

function Diagnosis({ status, busy, onRefresh }: { status: SupportStatus; busy: boolean; onRefresh: () => void }) {
  const report = status.kind === "ok" ? status.report : null;
  const alerts = Array.isArray(report?.alerts) ? (report.alerts as string[]) : [];
  return (
    <div className="support__diagnosis">
      <div className="support__heading">
        <h3>Diagnóstico</h3>
        <button type="button" onClick={onRefresh} disabled={busy}>
          Actualizar
        </button>
      </div>
      {status.kind === "loading" && <p className="operations__note">Pidiendo el diagnóstico…</p>}
      {status.kind === "error" && <p className="events__error">{status.message}</p>}
      {report && (
        <>
          <ul className="support__alerts" aria-label="Alertas">
            {alerts.length === 0 ? (
              <li className="pill pill--ok">Sin alertas</li>
            ) : (
              alerts.map((alert) => (
                <li key={alert} className="pill pill--down">
                  {alert}
                </li>
              ))
            )}
          </ul>
          <div className="operations__grid">
            {SECTIONS.map(({ key, label }) => (
              <Section key={key} label={label} data={report[key]} />
            ))}
          </div>
        </>
      )}
    </div>
  );
}

function CardResult({ card }: { card?: SupportCard }) {
  if (!card) return null;
  return (
    <div className="support__result">
      {card.error && <p className="events__error">{card.error}</p>}
      {card.plan && (
        <>
          {card.plan.refusal ? (
            <p className="events__error">No se puede: {card.plan.refusal}</p>
          ) : (
            <p className="operations__note">Simulación: no se ha escrito nada. Token {card.plan.token}.</p>
          )}
          <pre className="support__json">{JSON.stringify(card.plan.changes, null, 2)}</pre>
        </>
      )}
      {card.outcome && (
        <>
          <p className={card.outcome === "succeeded" ? "operations__note" : "events__error"}>Resultado: {card.outcome}</p>
          <pre className="support__json">{JSON.stringify(card.result, null, 2)}</pre>
        </>
      )}
    </div>
  );
}

function ActionCard({
  title,
  help,
  args,
  card,
  busy,
  onSimulate,
  onExecute,
  children,
}: {
  title: string;
  help: string;
  args: SupportArgs | null;
  card?: SupportCard;
  busy: boolean;
  onSimulate: (args: SupportArgs) => void;
  onExecute: (args: SupportArgs) => void;
  children: React.ReactNode;
}) {
  // «Ejecutar» solo tras una simulación sin negativa de estos mismos argumentos.
  const ready = args !== null && !!card?.plan?.token && !card.plan.refusal && card.argsKey === argsKey(args);
  return (
    <section className="operations__section support__card" aria-label={title}>
      <h3>{title}</h3>
      <p className="operations__note">{help}</p>
      <div className="support__fields">{children}</div>
      <div className="support__buttons">
        <button type="button" disabled={busy || args === null} onClick={() => args && onSimulate(args)}>
          Simular
        </button>
        <button type="button" disabled={busy || !ready} onClick={() => args && onExecute(args)}>
          Ejecutar
        </button>
      </div>
      <CardResult card={card} />
    </section>
  );
}

// Pantalla de soporte técnico: presentacional; las ejecuciones viven en useSupport.
export function SupportView(props: SupportViewProps) {
  const { enabled, components, views, status, cards, busy, onRefresh, onSimulate, onExecute, onBack } = props;
  const [mode, setMode] = useState<"truncated" | "recreated">("truncated");
  const [component, setComponent] = useState("");
  const [outboxId, setOutboxId] = useState("");
  const [view, setView] = useState("");
  const chosenComponent = component || components[0] || "";
  const chosenView = view || views[0] || "";
  const id = Number(outboxId);
  const replayArgs: SupportArgs | null =
    chosenComponent && Number.isInteger(id) && id >= 1
      ? { action: "replay-from", component: chosenComponent, outbox_id: id }
      : null;
  return (
    <section className="analytics support" aria-labelledby="support-title">
      <div className="analytics__top">
        <a
          className="analytics__back"
          href="#/"
          onClick={(e) => {
            e.preventDefault();
            onBack();
          }}
        >
          ← Vista principal
        </a>
        <h2 id="support-title" className="panel-title">
          Soporte técnico
        </h2>
      </div>
      {!enabled ? (
        <p className="operations__note support__disabled">
          El soporte técnico está deshabilitado: arranca el backend con WEB_SUPPORT=true.
        </p>
      ) : (
        <div className="support__body">
          <Diagnosis status={status} busy={busy} onRefresh={onRefresh} />
          <ActionCard
            title="Republicar tras perder el topic"
            help="Con el sistema parado y eda.events vacío: republica el outbox de todos los componentes en su orden."
            args={{ action: "republish", mode }}
            card={cards.republish}
            busy={busy}
            onSimulate={onSimulate}
            onExecute={onExecute}
          >
            <label>
              Modo{" "}
              <select value={mode} onChange={(e) => setMode(e.target.value as "truncated" | "recreated")}>
                <option value="truncated">truncated (vaciado con delete_records)</option>
                <option value="recreated">recreated (borrado y creado de nuevo)</option>
              </select>
            </label>
          </ActionCard>
          <ActionCard
            title="Republicar desde una fila"
            help="Marca como pendientes las filas del outbox de un componente desde un id: su relay las vuelve a publicar."
            args={replayArgs}
            card={cards["replay-from"]}
            busy={busy}
            onSimulate={onSimulate}
            onExecute={onExecute}
          >
            <label>
              Componente{" "}
              <select value={chosenComponent} onChange={(e) => setComponent(e.target.value)}>
                {components.map((name) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
            </label>
            <label>
              outbox_id <input type="number" min={1} value={outboxId} onChange={(e) => setOutboxId(e.target.value)} />
            </label>
          </ActionCard>
          <ActionCard
            title="Reconstruir una vista"
            help="Con eda-projector parado: vacía la vista y la vuelve a construir desde el topic; da las huellas antes y después."
            args={chosenView ? { action: "rebuild", view: chosenView } : null}
            card={cards.rebuild}
            busy={busy}
            onSimulate={onSimulate}
            onExecute={onExecute}
          >
            <label>
              Vista{" "}
              <select value={chosenView} onChange={(e) => setView(e.target.value)}>
                {views.map((name) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
            </label>
          </ActionCard>
        </div>
      )}
    </section>
  );
}
