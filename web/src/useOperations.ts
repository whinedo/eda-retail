import { useCallback, useEffect, useState } from "react";
import type {
  ApiClient,
  CarrierOperations,
  Freshness,
  IncidentMetricsRow,
  IncidentState,
  LlmStats,
  OperationsSummary,
} from "./api/client";

// Vista de operaciones (spec operations-view): como la de pedidos, un cambio aparece en menos de 2 segundos.
export const OPERATIONS_INTERVAL_MS = 1000;

export interface OperationsData {
  summary: OperationsSummary;
  incidents: IncidentMetricsRow[];
  nextBefore: string | null;
  llm: LlmStats;
  carriers: CarrierOperations;
  freshness: Freshness;
}

export interface OperationsState {
  data: OperationsData | null;
  error: string | null;
  state: IncidentState | null;
  reason: string | null;
  incidentId: string;
  paused: boolean;
  setState(state: IncidentState | null): void;
  setReason(reason: string | null): void;
  setIncidentId(incidentId: string): void;
  loadMore(): void;
  pause(): void;
  resume(): void;
}

// Contenedor: consulta /api/operations/* mientras la vista está abierta y no en pausa. Al cambiar un filtro consulta
// enseguida, también en pausa. «Cargar más» añade la página siguiente; el refresco en vivo renueva la primera y
// conserva las añadidas.
export function useOperations(client: ApiClient, enabled: boolean, intervalMs = OPERATIONS_INTERVAL_MS): OperationsState {
  const [data, setData] = useState<OperationsData | null>(null);
  const [extra, setExtra] = useState<{ rows: IncidentMetricsRow[]; nextBefore: string | null } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [state, setState] = useState<IncidentState | null>(null);
  const [reason, setReason] = useState<string | null>(null);
  const [incidentId, setIncidentId] = useState("");
  const [paused, setPaused] = useState(false);

  useEffect(() => setExtra(null), [state, reason, incidentId]);

  useEffect(() => {
    if (!enabled) return;
    let active = true;
    const id = incidentId || undefined;
    const refresh = () =>
      Promise.all([
        client.getOperationsSummary(id),
        client.getOperationsIncidents({ state: state ?? undefined, reason: reason ?? undefined, incidentId: id }),
        client.getOperationsLlm(),
        client.getOperationsCarriers(),
        client.getOperationsFreshness(),
      ]).then(
        ([summary, page, llm, carriers, freshness]) => {
          if (!active) return;
          setData({ summary, incidents: page.incidents, nextBefore: page.next_before, llm, carriers, freshness });
          setError(null);
        },
        (err: unknown) => active && setError(err instanceof Error ? err.message : String(err)),
      );
    refresh();
    const timer = paused ? undefined : setInterval(refresh, intervalMs);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [client, enabled, state, reason, incidentId, paused, intervalMs]);

  const nextBefore = extra ? extra.nextBefore : (data?.nextBefore ?? null);
  const loadMore = useCallback(() => {
    if (!nextBefore) return;
    client
      .getOperationsIncidents({
        state: state ?? undefined,
        reason: reason ?? undefined,
        incidentId: incidentId || undefined,
        before: nextBefore,
      })
      .then(
        (page) =>
          setExtra((prev) => ({ rows: [...(prev?.rows ?? []), ...page.incidents], nextBefore: page.next_before })),
        (err: unknown) => setError(err instanceof Error ? err.message : String(err)),
      );
  }, [client, nextBefore, state, reason, incidentId]);

  const merged = data && {
    ...data,
    incidents: [
      ...data.incidents,
      ...(extra?.rows ?? []).filter((r) => !data.incidents.some((d) => d.incident_id === r.incident_id)),
    ],
    nextBefore,
  };
  const pause = useCallback(() => setPaused(true), []);
  const resume = useCallback(() => setPaused(false), []);
  return { data: merged, error, state, reason, incidentId, paused, setState, setReason, setIncidentId, loadMore, pause, resume };
}
