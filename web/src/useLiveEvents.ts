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
// filtros por incidencia y nodo, pausa con búfer y páginas anteriores. `reset` vacía la vista
// y desde entonces solo sigue los eventos nuevos (no escribe nada en el backend).
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
  // Tras un reinicio no se pide la página de eventos: solo el stream. `floor` es el `seq` anterior
  // al primer evento recibido tras el reinicio (null hasta recibirlo: stream sin posición).
  const [resets, setResets] = useState(0);
  const floor = useRef<number | null>(null);

  useEffect(() => {
    const current = ++generation.current;
    let source: StreamLike | null = null;
    buffer.current = [];
    setRows([]);
    setNextBeforeSeq(null);
    setError(null);
    const follow = (afterSeq: number | undefined) => {
      source = openStream(eventsStreamUrl(afterSeq));
      source.addEventListener("event", (message) => {
        const row = JSON.parse(message.data) as EventRow;
        if (resets > 0 && floor.current === null) floor.current = row.seq - 1;
        // El mapa resalta la comunicación más reciente, aunque el panel la filtre o esté en pausa.
        setLastEvent((last) => (last && last.seq >= row.seq ? last : row));
        if (!matchesFilters(row, incidentId, node)) return;
        if (pausedRef.current) buffer.current.push(row);
        else setRows((rows) => mergeRows(rows, [row]));
      });
    };
    if (resets > 0) {
      follow(floor.current ?? undefined);
      return () => {
        generation.current++;
        source?.close();
      };
    }
    const filters = { incidentId: incidentId || undefined, node: node ?? undefined };
    client
      .getEvents({ limit: PAGE_SIZE, ...filters })
      .then((page) => {
        if (current !== generation.current) return;
        setRows(mergeRows([], page.events));
        setNextBeforeSeq(page.next_before_seq);
        follow(page.events.length ? Math.max(...page.events.map((r) => r.seq)) : undefined);
      })
      .catch((err) => {
        if (current === generation.current) setError(errorMessage(err));
      });
    return () => {
      generation.current++;
      source?.close();
    };
  }, [client, openStream, incidentId, node, resets]);

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

  const reset = useCallback(() => {
    floor.current = null;
    buffer.current = [];
    pausedRef.current = false;
    setPaused(false);
    setLastEvent(null);
    setIncidentId("");
    setNode(null);
    setResets((n) => n + 1);
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
    reset,
  };
}

export type LiveEvents = ReturnType<typeof useLiveEvents>;
