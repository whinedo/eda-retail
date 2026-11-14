import { type FormEvent, useEffect, useState } from "react";
import type { EventRow } from "../api/client";

export interface EventsPanelProps {
  rows: EventRow[];
  paused: boolean;
  incidentId: string;
  node: string | null;
  hasMore: boolean;
  error: string | null;
  jaegerUrl: string;
  onIncidentChange(incidentId: string): void;
  onClearNode(): void;
  onTogglePause(): void;
  onLoadMore(): void;
}

const ID_LENGTH = 8;
const PAYLOAD_LENGTH = 60;

const COLUMNS = [
  "Emisor",
  "Destino",
  "Tipo de evento",
  "Timestamp",
  "seq",
  "incident_id",
  "plan",
  "partición / offset",
  "event_id",
  "causation_id",
  "payload",
  "traza",
];

// Texto acortado con puntos suspensivos; el valor completo queda en el title.
function Truncated({ value, length }: { value: string; length: number }) {
  return <span title={value}>{value.length > length ? `${value.slice(0, length)}…` : value}</span>;
}

// 2026-11-14T01:48:19.234567+00:00 → 2026-11-14 01:48:19.234 (el valor completo, en el title).
function formatTimestamp(iso: string): string {
  return iso
    .replace("T", " ")
    .replace(/(\.\d{3})\d*/, "$1")
    .replace(/(Z|[+-]\d\d:\d\d)$/, "");
}

function EventsRow({ row, jaegerUrl }: { row: EventRow; jaegerUrl: string }) {
  return (
    <tr>
      <td title={row.sender}>{row.sender}</td>
      <td title={row.receivers.join(", ")}>{row.receiver}</td>
      <td className={row.event_type === "StepFailed" ? "event-type--failed" : undefined}>{row.event_type}</td>
      <td title={row.occurred_at}>{formatTimestamp(row.occurred_at)}</td>
      <td>{row.seq}</td>
      <td>{row.incident_id}</td>
      <td>{row.plan_version === null ? "—" : `v${row.plan_version}`}</td>
      <td>{row.partition === null || row.offset === null ? "—" : `p${row.partition} / ${row.offset}`}</td>
      <td>
        <Truncated value={row.event_id} length={ID_LENGTH} />
      </td>
      <td>{row.causation_id ? <Truncated value={row.causation_id} length={ID_LENGTH} /> : "—"}</td>
      <td>
        <Truncated value={JSON.stringify(row.payload)} length={PAYLOAD_LENGTH} />
      </td>
      <td>
        {row.trace_id ? (
          <a href={`${jaegerUrl}/trace/${row.trace_id}`} target="_blank" rel="noopener noreferrer" title={row.trace_id}>
            ver traza
          </a>
        ) : (
          "—"
        )}
      </td>
    </tr>
  );
}

// Panel de eventos intercambiados: presentacional; el estado y el stream viven en useLiveEvents.
export function EventsPanel(props: EventsPanelProps) {
  const { rows, paused, incidentId, node, hasMore, error } = props;
  const [draft, setDraft] = useState(incidentId);
  useEffect(() => setDraft(incidentId), [incidentId]);

  // El filtro de incidencia se aplica con Intro o al salir del campo, no en cada tecla.
  const applyIncident = (event?: FormEvent) => {
    event?.preventDefault();
    const value = draft.trim();
    if (value !== incidentId) props.onIncidentChange(value);
  };

  return (
    <section className="events" aria-labelledby="events-title">
      <div className="events__bar">
        <h2 id="events-title" className="panel-title">
          Eventos intercambiados
        </h2>
        <span className={paused ? "events__live events__live--paused" : "events__live"}>{paused ? "en pausa" : "en vivo"}</span>
        <span className="events__count">
          {rows.length.toLocaleString("es-ES")} {rows.length === 1 ? "evento" : "eventos"}
        </span>
        {node && (
          <span className="filter-chip">
            nodo: {node}
            <button type="button" className="link" aria-label={`Quitar filtro nodo: ${node}`} onClick={props.onClearNode}>
              ×
            </button>
          </span>
        )}
        <span className="events__hint">Desplaza hacia la derecha para ver más columnas</span>
        <form className="events__filter" onSubmit={applyIncident}>
          <label htmlFor="events-incident">Incidencia</label>
          <input
            id="events-incident"
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
        <table className="events__table">
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
            {rows.map((row) => (
              <EventsRow key={row.seq} row={row} jaegerUrl={props.jaegerUrl} />
            ))}
          </tbody>
        </table>
        {hasMore && (
          <button type="button" className="secondary events__more" onClick={props.onLoadMore}>
            Cargar más
          </button>
        )}
      </div>
    </section>
  );
}
