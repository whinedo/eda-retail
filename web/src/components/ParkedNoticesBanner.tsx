import { Fragment } from "react";
import type { ParkedNotice } from "../api/client";

// Plazo en hora local, p. ej. 20:13:36.
export function formatDeadline(iso: string): string {
  const date = new Date(iso);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

// I-3f2a9c1b-0000-… → I-3f2a9c1b (el id completo, en el title).
function shortIncident(incidentId: string): string {
  return incidentId.slice(0, 10);
}

// Aviso de avisos aparcados: presentacional; la consulta vive en useParkedNotices. Sin avisos no se muestra.
export function ParkedNoticesBanner({ notices }: { notices: ParkedNotice[] }) {
  if (notices.length === 0) return null;
  const many = notices.length > 1;
  return (
    <p className="parked-notices" role="status" aria-label="Avisos aparcados">
      ⚠ {notices.length} {many ? "avisos aparcados esperan" : "aviso aparcado espera"} pedidos que aún no existen:{" "}
      {notices.map((n, i) => (
        <Fragment key={n.incident_id}>
          {i > 0 && ", "}
          <strong>{n.order_ids.join(", ")}</strong> (
          <span className="parked-notices__incident" title={n.incident_id}>
            {shortIncident(n.incident_id)}
          </span>
          , vence <time dateTime={n.deadline_at}>{formatDeadline(n.deadline_at)}</time>)
        </Fragment>
      ))}
      . Si no has sembrado los pedidos, usa «seed».
    </p>
  );
}
