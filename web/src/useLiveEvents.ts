import { useCallback, useEffect, useRef, useState } from "react";
import { type ApiClient, ApiError, type EventRow, eventsStreamUrl } from "./api/client";
import { type OpenStream, openEventSource, type StreamLike } from "./useCommandRun";

export const PAGE_SIZE = 100;

// Mismas reglas que el backend: incidencia por `incident_id`; nodo emisor o cualquier destino.
export function matchesFilters(row: EventRow, incidentId: string, node: string | null): boolean {
  if (incidentId && row.incident_id !== incidentId) return false;
  if (node && row.sender !== node && !row.receivers.includes(node)) return false;
  return true;
}

// Une filas sin duplicar `seq`, de la más reciente a la más antigua.
export function mergeRows(current: EventRow[], incoming: EventRow[]): EventRow[] {
  const bySeq = new Map(current.map((r) => [r.seq, r]));
  for (const r of incoming) bySeq.set(r.seq, r);
  return [...bySeq.values()].sort((a, b) => b.seq - a.seq);
}

function errorMessage(error: unknown): string {
  return error instanceof ApiError ? error.message : String(error);
}

// Contenedor del panel de eventos: primera página, stream SSE desde el `seq` más alto,
// filtros por incidencia y nodo, pausa con búfer y páginas anteriores.
export function useLiveEvents(client: ApiClient, openStream: OpenStream = openEventSource) {
  const [incidentId, setIncidentId] = useState("");
  const [node, setNode] = useState<string | null>(null);
  const [rows, setRows] = useState<EventRow[]>([]);
  const [nextBeforeSeq, setNextBeforeSeq] = useState<number | null>(null);
  const [paused, setPaused] = useState(false);
  const [lastEvent, setLastEvent] = useState<EventRow | null>(null);
  const [error, setError] = useState<string | null>(null);
  const pausedRef = useRef(false);
  const buffer = useRef<EventRow[]>([]);
  // Cada cambio de filtro abre una generación nueva; las respuestas de la anterior se descartan.
  const generation = useRef(0);

  useEffect(() => {
    const current = ++generation.current;
    let source: StreamLike | null = null;
    buffer.current = [];
    setRows([]);
    setNextBeforeSeq(null);
    setError(null);
    const filters = { incidentId: incidentId || undefined, node: node ?? undefined };
    client
      .getEvents({ limit: PAGE_SIZE, ...filters })
      .then((page) => {
        if (current !== generation.current) return;
        setRows(mergeRows([], page.events));
        setNextBeforeSeq(page.next_before_seq);
        const highest = page.events.length ? Math.max(...page.events.map((r) => r.seq)) : undefined;
        source = openStream(eventsStreamUrl(highest));
        source.addEventListener("event", (message) => {
          const row = JSON.parse(message.data) as EventRow;
          // El mapa resalta la comunicación más reciente, aunque el panel la filtre o esté en pausa.
          setLastEvent((last) => (last && last.seq >= row.seq ? last : row));
          if (!matchesFilters(row, incidentId, node)) return;
          if (pausedRef.current) buffer.current.push(row);
          else setRows((rows) => mergeRows(rows, [row]));
        });
      })
      .catch((err) => {
        if (current === generation.current) setError(errorMessage(err));
      });
    return () => {
      generation.current++;
      source?.close();
    };
  }, [client, openStream, incidentId, node]);

  const pause = useCallback(() => {
    pausedRef.current = true;
    setPaused(true);
  }, []);

  const resume = useCallback(() => {
    pausedRef.current = false;
    setPaused(false);
    const buffered = buffer.current;
    buffer.current = [];
    setRows((rows) => mergeRows(rows, buffered));
  }, []);

  const loadMore = useCallback(async () => {
    if (nextBeforeSeq === null) return;
    const current = generation.current;
    try {
      const page = await client.getEvents({
        limit: PAGE_SIZE,
        beforeSeq: nextBeforeSeq,
        incidentId: incidentId || undefined,
        node: node ?? undefined,
      });
      if (current !== generation.current) return;
      setRows((rows) => mergeRows(rows, page.events));
      setNextBeforeSeq(page.next_before_seq);
    } catch (err) {
      if (current === generation.current) setError(errorMessage(err));
    }
  }, [client, nextBeforeSeq, incidentId, node]);

  return {
    rows,
    total: rows.length,
    paused,
    pause,
    resume,
    incidentId,
    setIncidentId,
    node,
    setNode,
    hasMore: nextBeforeSeq !== null,
    loadMore,
    lastEvent,
    error,
  };
}

export type LiveEvents = ReturnType<typeof useLiveEvents>;
