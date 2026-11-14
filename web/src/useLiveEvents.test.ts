import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ApiClient, EventRow, EventsPage, EventsQuery } from "./api/client";
import type { StreamLike } from "./useCommandRun";
import { useLiveEvents } from "./useLiveEvents";

class FakeStream implements StreamLike {
  static all: FakeStream[] = [];
  readonly listeners = new Map<string, (e: MessageEvent) => void>();
  closed = false;

  constructor(readonly url: string) {
    FakeStream.all.push(this);
  }

  static get last(): FakeStream {
    return FakeStream.all[FakeStream.all.length - 1];
  }

  addEventListener(type: string, listener: (e: MessageEvent) => void) {
    this.listeners.set(type, listener);
  }

  close() {
    this.closed = true;
  }

  emit(row: EventRow) {
    act(() => this.listeners.get("event")?.(new MessageEvent("event", { data: JSON.stringify(row), lastEventId: String(row.seq) })));
  }
}

function row(seq: number, overrides: Partial<EventRow> = {}): EventRow {
  return {
    seq,
    event_id: `ev-${seq}`,
    event_type: "PlanCreated",
    occurred_at: "2026-11-14T01:48:19.234567+00:00",
    sender: "planner",
    receiver: "orchestrator",
    receivers: ["orchestrator", "projectors"],
    incident_id: "INC-0007",
    plan_version: 1,
    partition: null,
    offset: null,
    causation_id: null,
    payload: {},
    trace_id: null,
    ...overrides,
  };
}

function setup(pages: (query: EventsQuery) => EventsPage = () => ({ events: [row(1303), row(1302)], next_before_seq: 1302 })) {
  FakeStream.all = [];
  const getEvents = vi.fn(async (query: EventsQuery = {}) => pages(query));
  const client = { getEvents } as unknown as ApiClient;
  const openStream = vi.fn((url: string) => new FakeStream(url));
  const hook = renderHook(() => useLiveEvents(client, openStream));
  return { getEvents, openStream, ...hook };
}

const seqs = (rows: EventRow[]) => rows.map((r) => r.seq);

describe("useLiveEvents", () => {
  it("carga la primera página y abre el stream tras el seq más alto", async () => {
    const { getEvents, openStream, result } = setup();

    await waitFor(() => expect(openStream).toHaveBeenCalledWith("/api/events/stream?after_seq=1303"));
    expect(getEvents).toHaveBeenCalledWith({ limit: 100, incidentId: undefined, node: undefined });
    expect(seqs(result.current.rows)).toEqual([1303, 1302]);
    expect(result.current.hasMore).toBe(true);
  });

  it("sin eventos cargados abre el stream sin posición", async () => {
    const { openStream } = setup(() => ({ events: [], next_before_seq: null }));

    await waitFor(() => expect(openStream).toHaveBeenCalledWith("/api/events/stream"));
  });

  it("Eventos en vivo: añade por seq, sin duplicados, y en pausa no añade hasta reanudar", async () => {
    const { openStream, result } = setup();
    await waitFor(() => expect(openStream).toHaveBeenCalled());

    FakeStream.last.emit(row(1305));
    FakeStream.last.emit(row(1304));
    FakeStream.last.emit(row(1305));
    expect(seqs(result.current.rows)).toEqual([1305, 1304, 1303, 1302]);
    expect(result.current.lastEvent?.seq).toBe(1305);

    act(() => result.current.pause());
    expect(result.current.paused).toBe(true);
    FakeStream.last.emit(row(1306));
    expect(seqs(result.current.rows)).toEqual([1305, 1304, 1303, 1302]);
    expect(result.current.lastEvent?.seq).toBe(1306);

    act(() => result.current.resume());
    expect(result.current.paused).toBe(false);
    expect(seqs(result.current.rows)).toEqual([1306, 1305, 1304, 1303, 1302]);
  });

  it("el filtro de incidencia recarga la página y reabre el stream con el nuevo after_seq", async () => {
    const { getEvents, openStream, result } = setup((query) =>
      query.incidentId === "INC-0006"
        ? { events: [row(1292, { incident_id: "INC-0006" })], next_before_seq: null }
        : { events: [row(1303)], next_before_seq: null },
    );
    await waitFor(() => expect(openStream).toHaveBeenCalledTimes(1));
    const first = FakeStream.last;

    act(() => result.current.setIncidentId("INC-0006"));

    await waitFor(() => expect(openStream).toHaveBeenLastCalledWith("/api/events/stream?after_seq=1292"));
    expect(getEvents).toHaveBeenLastCalledWith({ limit: 100, incidentId: "INC-0006", node: undefined });
    expect(first.closed).toBe(true);
    expect(seqs(result.current.rows)).toEqual([1292]);
  });

  it("no muestra eventos en vivo que no cumplen los filtros de incidencia y nodo", async () => {
    const { openStream, result } = setup(() => ({ events: [], next_before_seq: null }));
    await waitFor(() => expect(openStream).toHaveBeenCalledTimes(1));

    act(() => {
      result.current.setIncidentId("INC-0007");
      result.current.setNode("planner");
    });
    await waitFor(() => expect(openStream).toHaveBeenCalledTimes(2));

    FakeStream.last.emit(row(10, { incident_id: "INC-0006" }));
    FakeStream.last.emit(row(11, { sender: "interpreter", receiver: "orchestrator", receivers: ["orchestrator", "projectors"] }));
    FakeStream.last.emit(row(12, { sender: "interpreter", receiver: "planner", receivers: ["planner", "projectors"] }));
    FakeStream.last.emit(row(13));
    expect(seqs(result.current.rows)).toEqual([13, 12]);
  });

  it("Cargar más pide la página anterior con beforeSeq y la añade al final", async () => {
    const { getEvents, openStream, result } = setup((query) =>
      query.beforeSeq === 1302
        ? { events: [row(1301), row(1300)], next_before_seq: null }
        : { events: [row(1303), row(1302)], next_before_seq: 1302 },
    );
    await waitFor(() => expect(openStream).toHaveBeenCalled());

    await act(() => result.current.loadMore());

    expect(getEvents).toHaveBeenLastCalledWith({ limit: 100, beforeSeq: 1302, incidentId: undefined, node: undefined });
    expect(seqs(result.current.rows)).toEqual([1303, 1302, 1301, 1300]);
    expect(result.current.hasMore).toBe(false);
  });

  it("cierra el stream al desmontar", async () => {
    const { openStream, unmount } = setup();
    await waitFor(() => expect(openStream).toHaveBeenCalled());

    unmount();

    expect(FakeStream.last.closed).toBe(true);
  });
});
