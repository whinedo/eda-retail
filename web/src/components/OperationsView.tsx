import { type FormEvent, useEffect, useState } from "react";
import type { IncidentMetricsRow, IncidentState } from "../api/client";
import type { OperationsData } from "../useOperations";

// Estados de incident_metrics con las etiquetas y colores de la vista de pedidos (spec operations-view).
export const STATES: { state: IncidentState; label: string }[] = [
  { state: "open", label: "Abierta" },
  { state: "resolved", label: "Solucionada" },
  { state: "refunded", label: "Reembolsada" },
  { state: "escalated", label: "Escalada" },
];
const LABELS = Object.fromEntries(STATES.map((s) => [s.state, s.label])) as Record<IncidentState, string>;

export const PRICE_VARIABLES = ["LLM_PRICE_IN_PER_1K", "LLM_PRICE_OUT_PER_1K", "EFFECT_COST_CLAIM", "EFFECT_COST_REISSUE"];

const COLUMNS = [
  "Incidencia",
  "Estado",
  "Motivo de escalado",
  "Transportista",
  "Causa / gravedad",
  "Versión del plan",
  "Pasos fallidos",
  "Compensaciones",
  "Duración",
  "Tokens",
  "Coste",
  "Último evento",
  "",
];

export interface OperationsViewProps {
  data: OperationsData | null;
  error: string | null;
  state: IncidentState | null;
  reason: string | null;
  incidentId: string;
  paused: boolean;
  onStateChange(state: IncidentState | null): void;
  onReasonChange(reason: string | null): void;
  onIncidentChange(incidentId: string): void;
  onTogglePause(): void;
  onLoadMore(): void;
  onViewEvents(incidentId: string): void;
  onOrders(): void;
  onProfit(): void;
  onBack(): void;
}

const number = (n: number) => n.toLocaleString("es-ES");
const money = (value: string | number) =>
  `${Number(value).toLocaleString("es-ES", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} €`;
const ms = (value: number | null) => (value === null ? "—" : `${number(Math.round(value))} ms`);

export function formatDuration(seconds: number | null): string {
  if (seconds === null) return "";
  if (seconds < 60) return `${seconds.toLocaleString("es-ES", { maximumFractionDigits: 1 })} s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes} min ${Math.round(seconds - minutes * 60)} s`;
}

function totalCost(row: IncidentMetricsRow): number {
  return Number(row.llm_cost) + Number(row.effects_cost) + Number(row.refunded);
}

function link(label: string, onClick: () => void, href: string) {
  return (
    <a
      className="analytics__back"
      href={href}
      onClick={(e) => {
        e.preventDefault();
        onClick();
      }}
    >
      {label}
    </a>
  );
}

// Vista de operaciones: presentacional; el estado y las consultas viven en useOperations.
export function OperationsView(props: OperationsViewProps) {
  const { data, error, state, reason, incidentId, paused } = props;
  const [draft, setDraft] = useState(incidentId);
  useEffect(() => setDraft(incidentId), [incidentId]);
  const summary = data?.summary;
  const incidents = data?.incidents ?? [];
  const escalated = summary?.escalations.reduce((sum, e) => sum + e.count, 0) ?? 0;

  const applyIncident = (event?: FormEvent) => {
    event?.preventDefault();
    const value = draft.trim();
    if (value !== incidentId) props.onIncidentChange(value);
  };

  return (
    <section className="analytics operations" aria-labelledby="operations-title">
      <div className="analytics__top">
        {link("← Vista principal", props.onBack, "#/")}
        {link("← Estado de los pedidos", props.onOrders, "#/analytics")}
        <h2 id="operations-title" className="panel-title">
          Operaciones
        </h2>
        {link("Beneficio", props.onProfit, "#/analytics/operations/profit")}
      </div>
      <ul className="analytics__counters" aria-label="Incidencias por estado">
        {STATES.map(({ state: value, label }) => (
          <li key={value}>
            <button
              type="button"
              className={`counter counter--${value}`}
              aria-pressed={state === value}
              title="Pulsa para filtrar la tabla"
              onClick={() => props.onStateChange(state === value ? null : value)}
            >
              <strong>{number(summary?.by_state[value] ?? 0)}</strong>
              <span>{label}</span>
            </button>
          </li>
        ))}
      </ul>
      <div className="operations__section" role="group" aria-label="Escaladas por motivo">
        <h3>Escaladas por motivo</h3>
        {escalated === 0 ? (
          <p className="analytics__empty">Sin incidencias escaladas</p>
        ) : (
          <ul className="operations__bars">
            {summary?.escalations.map((e) => {
              const pct = Math.round((100 * e.count) / escalated);
              return (
                <li key={e.reason}>
                  <button
                    type="button"
                    className="operations__bar"
                    aria-pressed={reason === e.reason}
                    onClick={() => props.onReasonChange(reason === e.reason ? null : e.reason)}
                  >
                    <span className="operations__bar-label">{e.reason}</span>
                    <span className="operations__bar-fill" style={{ width: `${pct}%` }} aria-hidden="true" />
                    <span>{`${number(e.count)} (${pct} %)`}</span>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </div>
      <div className="events__bar">
        <span className={paused ? "events__live events__live--paused" : "events__live"}>{paused ? "en pausa" : "en vivo"}</span>
        <span className="events__count">
          {number(incidents.length)} {incidents.length === 1 ? "incidencia" : "incidencias"}
        </span>
        {state && (
          <span className="filter-chip">
            estado: {LABELS[state]}
            <button type="button" className="link" aria-label={`Quitar filtro estado: ${LABELS[state]}`} onClick={() => props.onStateChange(null)}>
              ×
            </button>
          </span>
        )}
        {reason && (
          <span className="filter-chip">
            motivo: {reason}
            <button type="button" className="link" aria-label={`Quitar filtro motivo: ${reason}`} onClick={() => props.onReasonChange(null)}>
              ×
            </button>
          </span>
        )}
        {summary && (
          <span className="events__hint" title="Posición de Kafka hasta la que ha leído la vista (consistencia eventual)">
            leído hasta {Object.entries(summary.applied).map(([p, o]) => `p${p} ${o}`).join(" · ") || "—"}
          </span>
        )}
        <form className="events__filter" onSubmit={applyIncident}>
          <label htmlFor="operations-incident">Incidencia</label>
          <input id="operations-incident" value={draft} placeholder="INC-0007" onChange={(e) => setDraft(e.target.value)} onBlur={() => applyIncident()} />
        </form>
        <button type="button" className="secondary" onClick={props.onTogglePause}>
          {paused ? "Reanudar" : "Pausar"}
        </button>
      </div>
      {error && <p className="events__error">{error}</p>}
      <div className="operations__body">
        <div className="events__scroll" role="group" aria-label="Tabla de incidencias">
          <table className="events__table operations__table">
            <thead className="events__head">
              <tr>
                {COLUMNS.map((column, i) => (
                  <th key={column || i} scope="col">
                    {column}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {incidents.map((row) => (
                <tr key={row.incident_id}>
                  <td>{row.incident_id}</td>
                  <td className={`status status--${row.state}`}>{LABELS[row.state]}</td>
                  <td>{row.escalation_reason ?? ""}</td>
                  <td>{row.carrier}</td>
                  <td>{row.cause ? `${row.cause} / ${row.severity}` : ""}</td>
                  <td>{row.plan_version}</td>
                  <td>{row.steps_failed}</td>
                  <td>{row.compensations}</td>
                  <td>{formatDuration(row.duration_s)}</td>
                  <td>{number(row.tokens)}</td>
                  <td>{money(totalCost(row))}</td>
                  <td title={row.last_event_id}>{`${row.last_event_type} · p${row.kafka_partition} / ${row.kafka_offset}`}</td>
                  <td>
                    <button type="button" className="link" onClick={() => props.onViewEvents(row.incident_id)}>
                      Ver eventos
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {data && incidents.length === 0 && <p className="analytics__empty">Sin incidencias todavía</p>}
          {data?.nextBefore && (
            <button type="button" className="secondary events__more" onClick={props.onLoadMore}>
              Cargar más
            </button>
          )}
        </div>
        {data && <Sections data={data} />}
      </div>
    </section>
  );
}

function Sections({ data }: { data: OperationsData }) {
  const { summary, llm, carriers, freshness } = data;
  return (
    <div className="operations__grid">
      <section className="operations__section" aria-label="Coste por escenario">
        <h3>Coste por escenario</h3>
        {!summary.prices_configured && (
          <p className="operations__note">
            El coste en dinero es 0 porque no hay precios configurados ({PRICE_VARIABLES.join(", ")}); los tokens sí
            se cuentan.
          </p>
        )}
        <table className="events__table">
          <thead>
            <tr>
              <th scope="col">Causa / gravedad</th>
              <th scope="col">Incidencias</th>
              <th scope="col">Tokens medios</th>
              <th scope="col">Coste LLM</th>
              <th scope="col">Coste efectos</th>
              <th scope="col">Reembolsado</th>
            </tr>
          </thead>
          <tbody>
            {summary.cost_by_scenario.map((c) => (
              <tr key={`${c.cause}/${c.severity}`}>
                <td>{c.cause ? `${c.cause} / ${c.severity}` : "Sin interpretar"}</td>
                <td>{number(c.incidents)}</td>
                <td className={summary.prices_configured ? undefined : "operations__highlight"}>{number(Math.round(c.mean_tokens))}</td>
                <td>{money(c.llm_cost)}</td>
                <td>{money(c.effects_cost)}</td>
                <td>{money(c.refunded)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <section className="operations__section" aria-label="Llamadas al LLM por agente">
        <h3>Llamadas al LLM por agente</h3>
        <table className="events__table">
          <thead>
            <tr>
              <th scope="col">Agente</th>
              <th scope="col">Llamadas</th>
              <th scope="col">Tokens entrada</th>
              <th scope="col">Tokens salida</th>
              <th scope="col">Latencia media</th>
              <th scope="col" title="Se calcula por rango más próximo: el valor en la posición ⌈0,95·N⌉ de las N latencias ordenadas">
                p95
              </th>
              <th scope="col">Validaciones superadas</th>
            </tr>
          </thead>
          <tbody>
            {llm.agents.map((a) => (
              <tr key={a.agent}>
                <td>{a.agent}</td>
                <td>{number(a.calls)}</td>
                <td>{number(a.tokens_in)}</td>
                <td>{number(a.tokens_out)}</td>
                <td>{ms(a.latency_mean_ms)}</td>
                <td>{ms(a.latency_p95_ms)}</td>
                <td>{a.validated_pct === null ? "—" : `${Math.round(a.validated_pct)} %`}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <section className="operations__section" aria-label="Replanificación y pasos fallidos">
        <h3>Replanificación y pasos fallidos</h3>
        <ul className="operations__versions" aria-label="Incidencias por versión del plan">
          {Object.entries(summary.by_plan_version).map(([version, count]) => (
            <li key={version}>{`v${version}: ${number(count)}`}</li>
          ))}
        </ul>
        <table className="events__table">
          <thead>
            <tr>
              <th scope="col">Acción</th>
              <th scope="col">Error</th>
              <th scope="col">Fallos</th>
            </tr>
          </thead>
          <tbody>
            {summary.step_failures.map((f) => (
              <tr key={`${f.action}/${f.error_code}`}>
                <td>{f.action}</td>
                <td>{f.error_code}</td>
                <td>{number(f.count)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <section className="operations__section" aria-label="Transportistas">
        <h3>Transportistas</h3>
        <table className="events__table">
          <thead>
            <tr>
              <th scope="col">Transportista</th>
              <th scope="col">Incidencias</th>
              <th scope="col">Retrasos</th>
              <th scope="col">Reclamaciones</th>
              <th scope="col">Reembolsos</th>
            </tr>
          </thead>
          <tbody>
            {carriers.carriers.map((c) => (
              <tr key={c.carrier}>
                <td>{c.carrier}</td>
                <td>{number(c.incidents)}</td>
                <td>{number(c.delays)}</td>
                <td>{number(c.claims_filed)}</td>
                <td>{number(c.refunds_issued)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {carriers.consults.length === 0 ? (
          <p className="operations__note">Las consultas al agente del transportista solo existen con POC_VERSION=2.</p>
        ) : (
          <table className="events__table" aria-label="Consultas A2A">
            <thead>
              <tr>
                <th scope="col">Resultado</th>
                <th scope="col">Consultas</th>
                <th scope="col">Rondas medias</th>
                <th scope="col">Latencia media</th>
              </tr>
            </thead>
            <tbody>
              {carriers.consults.map((c) => (
                <tr key={c.outcome}>
                  <td>{c.outcome}</td>
                  <td>{number(c.consults)}</td>
                  <td>{c.mean_rounds.toLocaleString("es-ES", { maximumFractionDigits: 1 })}</td>
                  <td>{ms(c.mean_latency_ms)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
      <section className="operations__section" aria-label="Frescura de las vistas">
        <h3>Frescura de las vistas</h3>
        <p className="operations__note">
          Las cifras de esta vista van por detrás de los eventos (consistencia eventual): el lag mide cuántos mensajes de
          Kafka le faltan por aplicar a cada proyector.
        </p>
        <table className="events__table">
          <thead>
            <tr>
              <th scope="col">Proyector</th>
              <th scope="col">Partición</th>
              <th scope="col">Aplicado</th>
              <th scope="col">End offset</th>
              <th scope="col">Lag</th>
            </tr>
          </thead>
          <tbody>
            {freshness.consumers.flatMap((c) => [
              ...c.partitions.map((p) => (
                <tr key={`${c.consumer}/${p.partition}`}>
                  <td>{c.consumer}</td>
                  <td>{p.partition}</td>
                  <td>{p.applied ?? "—"}</td>
                  <td>{p.end_offset ?? "—"}</td>
                  <Lag value={p.lag} />
                </tr>
              )),
              <tr key={`${c.consumer}/total`} className="operations__total">
                <td>{c.consumer}</td>
                <td>total</td>
                <td />
                <td />
                <Lag value={c.lag} />
              </tr>,
            ])}
          </tbody>
        </table>
      </section>
    </div>
  );
}

function Lag({ value }: { value: number | null }) {
  if (value === null) return <td className="operations__nodata">sin datos</td>;
  return <td className={value > 0 ? "operations__lag" : undefined}>{number(value)}</td>;
}
