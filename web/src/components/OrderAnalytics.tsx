import { type FormEvent, useEffect, useState } from "react";
import type { OrderAnalytics as OrderAnalyticsData, OrderStatus } from "../api/client";

// Estados de order_status con su etiqueta y el evento real que los produce (spec cqrs-views).
export const STATUSES: { status: OrderStatus; label: string; event: string }[] = [
  { status: "delayed", label: "Envío retrasado", event: "ShipmentDelayed" },
  { status: "in_resolution", label: "En proceso de solución", event: "PlanCreated / PlanRevised" },
  { status: "resolved", label: "Solucionada", event: "IncidentResolved resolved" },
  { status: "refunded", label: "Reembolsada", event: "IncidentResolved refunded" },
  { status: "escalated", label: "Escalada", event: "IncidentEscalated" },
];

const LABELS = Object.fromEntries(STATUSES.map((s) => [s.status, s.label])) as Record<OrderStatus, string>;

const COLUMNS = ["Pedido", "Estado", "Incidencia", "Último evento", "Timestamp", "partición / offset", "event_id"];

export interface OrderAnalyticsProps {
  data: OrderAnalyticsData | null;
  error: string | null;
  status: OrderStatus | null;
  incidentId: string;
  paused: boolean;
  onStatusChange(status: OrderStatus | null): void;
  onIncidentChange(incidentId: string): void;
  onTogglePause(): void;
  onBack(): void;
  // Abre la vista de operaciones (spec operations-view); sin él no hay enlace.
  onOperations?(): void;
}

// 2026-11-14T01:48:19.234567+00:00 → 2026-11-14 01:48:19.234 (el valor completo, en el title).
function formatTimestamp(iso: string): string {
  return iso
    .replace("T", " ")
    .replace(/(\.\d{3})\d*/, "$1")
    .replace(/(Z|[+-]\d\d:\d\d)$/, "");
}

// Vista de analítica de pedidos: presentacional; el estado y la consulta viven en useOrderAnalytics.
export function OrderAnalytics(props: OrderAnalyticsProps) {
  const { data, error, status, incidentId, paused } = props;
  const [draft, setDraft] = useState(incidentId);
  useEffect(() => setDraft(incidentId), [incidentId]);
  const orders = data?.orders ?? [];

  // Como en el panel de eventos: el filtro de incidencia se aplica con Intro o al salir del campo.
  const applyIncident = (event?: FormEvent) => {
    event?.preventDefault();
    const value = draft.trim();
    if (value !== incidentId) props.onIncidentChange(value);
  };

  return (
    <section className="analytics" aria-labelledby="analytics-title">
      <div className="analytics__top">
        <a
          className="analytics__back"
          href="#/"
          onClick={(e) => {
            e.preventDefault();
            props.onBack();
          }}
        >
          ← Vista principal
        </a>
        <h2 id="analytics-title" className="panel-title">
          Estado de los pedidos
        </h2>
        {props.onOperations && (
          <a
            className="analytics__back"
            href="#/analytics/operations"
            onClick={(e) => {
              e.preventDefault();
              props.onOperations?.();
            }}
          >
            Operaciones
          </a>
        )}
      </div>
      <ul className="analytics__counters" aria-label="Pedidos por estado">
        {STATUSES.map(({ status: value, label, event }) => (
          <li key={value}>
            <button
              type="button"
              className={`counter counter--${value}`}
              aria-pressed={status === value}
              title={`${event}. Pulsa para filtrar la tabla`}
              onClick={() => props.onStatusChange(status === value ? null : value)}
            >
              <strong>{(data?.counts[value] ?? 0).toLocaleString("es-ES")}</strong>
              <span>{label}</span>
            </button>
          </li>
        ))}
      </ul>
      <div className="events__bar">
        <span className={paused ? "events__live events__live--paused" : "events__live"}>{paused ? "en pausa" : "en vivo"}</span>
        <span className="events__count">
          {orders.length.toLocaleString("es-ES")} {orders.length === 1 ? "pedido" : "pedidos"}
        </span>
        {status && (
          <span className="filter-chip">
            estado: {LABELS[status]}
            <button
              type="button"
              className="link"
              aria-label={`Quitar filtro estado: ${LABELS[status]}`}
              onClick={() => props.onStatusChange(null)}
            >
              ×
            </button>
          </span>
        )}
        <span className="events__hint">Desplaza hacia la derecha para ver más columnas</span>
        <form className="events__filter" onSubmit={applyIncident}>
          <label htmlFor="analytics-incident">Incidencia</label>
          <input
            id="analytics-incident"
            value={draft}
            placeholder="INC-0007"
            onChange={(e) => setDraft(e.target.value)}
            onBlur={() => applyIncident()}
          />
        </form>
        <button type="button" className="secondary" onClick={props.onTogglePause}>
          {paused ? "Reanudar" : "Pausar"}
        </button>
      </div>
      {error && <p className="events__error">{error}</p>}
      <div className="events__scroll">
        <table className="events__table analytics__table">
          <thead className="events__head">
            <tr>
              {COLUMNS.map((column) => (
                <th key={column} scope="col">
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {orders.map((row) => (
              <tr key={`${row.order_id}/${row.incident_id}`}>
                <td>{row.order_id}</td>
                <td className={`status status--${row.status}`}>{LABELS[row.status]}</td>
                <td>{row.incident_id}</td>
                <td>{row.last_event_type}</td>
                <td title={row.occurred_at}>{formatTimestamp(row.occurred_at)}</td>
                <td>{`p${row.kafka_partition} / ${row.kafka_offset}`}</td>
                <td title={row.last_event_id}>{row.last_event_id.slice(0, 8)}…</td>
              </tr>
            ))}
          </tbody>
        </table>
        {data && orders.length === 0 && <p className="analytics__empty">Sin pedidos afectados por incidencias.</p>}
      </div>
    </section>
  );
}
