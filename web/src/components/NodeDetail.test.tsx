import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { ApiError, type ApiClient, type EventRow, type EventsQuery, type NodeDetail as NodeDetailData } from "../api/client";
import type { StreamLike } from "../useCommandRun";
import { useLiveEvents } from "../useLiveEvents";
import { EventsPanel } from "./EventsPanel";
import { NodeDetailPanel } from "./NodeDetail";

class FakeStream implements StreamLike {
  closed = false;
  addEventListener() {}
  close() {
    this.closed = true;
  }
}

const PLANNER: NodeDetailData = {
  id: "planner",
  label: "Agente planificador",
  kind: "llm",
  description: "Propone el plan de recuperación.",
  monitored: true,
  up: true,
  consumes: ["ShipmentDelayed"],
  emits: ["PlanCreated"],
  llm: { calls: 2, tokens_in: 400, tokens_out: 60, avg_latency_ms: 2000, validations_passed: 1 },
};

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

function client(overrides: Partial<ApiClient> = {}): ApiClient {
  return {
    getNode: vi.fn(async () => PLANNER),
    getEvents: vi.fn(async (query: EventsQuery = {}) =>
      query.node === "planner"
        ? { events: [row(1303)], next_before_seq: null }
        : { events: [row(1303), row(1291, { event_type: "StepCompleted", sender: "executor-file_claim" })], next_before_seq: null },
    ),
    ...overrides,
  } as unknown as ApiClient;
}

const openStream = () => new FakeStream();

// Mismo cableado que App: el detalle fija el filtro de nodo del panel de eventos.
function Harness({ api }: { api: ApiClient }) {
  const events = useLiveEvents(api, openStream);
  const [selected, setSelected] = useState<string | null>("planner");
  return (
    <>
      {selected && (
        <NodeDetailPanel client={api} nodeId={selected} onFilter={events.setNode} onClose={() => setSelected(null)} />
      )}
      <EventsPanel
        jaegerUrl="http://localhost:16686"
        rows={events.rows}
        paused={events.paused}
        incidentId={events.incidentId}
        node={events.node}
        hasMore={events.hasMore}
        error={events.error}
        onIncidentChange={events.setIncidentId}
        onClearNode={() => events.setNode(null)}
        onTogglePause={events.paused ? events.resume : events.pause}
        onLoadMore={() => void events.loadMore()}
      />
    </>
  );
}

describe("Detalle de un nodo", () => {
  it("Filtrar eventos por nodo: el panel inferior muestra solo los del planificador", async () => {
    const api = client();
    const user = userEvent.setup();
    render(<Harness api={api} />);
    expect(await screen.findByText("StepCompleted")).toBeInTheDocument();

    await user.click(await screen.findByRole("button", { name: "Filtrar eventos" }));

    await waitFor(() => expect(screen.queryByText("StepCompleted")).not.toBeInTheDocument());
    expect(api.getEvents).toHaveBeenLastCalledWith({ limit: 100, incidentId: undefined, node: "planner" });
    expect(screen.getByText("PlanCreated", { selector: "td *, td" })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Quitar filtro nodo: planner" }));
    expect(await screen.findByText("StepCompleted")).toBeInTheDocument();
    expect(api.getEvents).toHaveBeenLastCalledWith({ limit: 100, incidentId: undefined, node: undefined });
  });

  it("Métricas de un agente con LLM: llamadas, tokens, latencia media y validaciones", async () => {
    const api = client();
    render(<NodeDetailPanel client={api} nodeId="planner" onFilter={vi.fn()} onClose={vi.fn()} />);

    const panel = await screen.findByRole("complementary", { name: "Agente planificador" });
    expect(api.getNode).toHaveBeenCalledWith("planner");
    expect(panel).toHaveTextContent("Agente con LLM");
    expect(panel).toHaveTextContent("Propone el plan de recuperación.");
    expect(panel).toHaveTextContent("activo");
    expect(panel).toHaveTextContent("ShipmentDelayed");
    expect(panel).toHaveTextContent("PlanCreated");
    expect(screen.getByText("Llamadas").nextSibling).toHaveTextContent("2");
    expect(screen.getByText("Tokens de entrada").nextSibling).toHaveTextContent("400");
    expect(screen.getByText("Tokens de salida").nextSibling).toHaveTextContent("60");
    expect(screen.getByText("Latencia media").nextSibling).toHaveTextContent("2,0 s");
    expect(screen.getByText("Validaciones superadas").nextSibling).toHaveTextContent("1");
  });

  it("sin LLM no muestra métricas y distingue caído y sin proceso propio", async () => {
    const api = client({
      getNode: vi.fn(async (id: string) =>
        id === "relay"
          ? { ...PLANNER, id, label: "Relay", kind: "code" as const, up: false, llm: null }
          : { ...PLANNER, id, label: "Kafka", kind: "data" as const, up: null, llm: null },
      ),
    });
    const { rerender } = render(<NodeDetailPanel client={api} nodeId="relay" onFilter={vi.fn()} onClose={vi.fn()} />);
    expect(await screen.findByText("caído")).toBeInTheDocument();
    expect(screen.queryByText("Llamadas")).not.toBeInTheDocument();

    rerender(<NodeDetailPanel client={api} nodeId="kafka" onFilter={vi.fn()} onClose={vi.fn()} />);
    expect(await screen.findByText("sin proceso propio")).toBeInTheDocument();
  });

  it("un nodo desconocido o un error muestran un aviso corto", async () => {
    const api = client({ getNode: vi.fn(async () => Promise.reject(new ApiError(404, "nodo desconocido"))) });
    const { rerender } = render(<NodeDetailPanel client={api} nodeId="nope" onFilter={vi.fn()} onClose={vi.fn()} />);
    expect(await screen.findByText("Nodo desconocido: nope")).toBeInTheDocument();

    const failing = client({ getNode: vi.fn(async () => Promise.reject(new Error("boom"))) });
    rerender(<NodeDetailPanel client={failing} nodeId="planner" onFilter={vi.fn()} onClose={vi.fn()} />);
    expect(await screen.findByText("No se pudo cargar el detalle del nodo")).toBeInTheDocument();
  });

  it("el botón de cerrar avisa al contenedor", async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(<NodeDetailPanel client={client()} nodeId="planner" onFilter={vi.fn()} onClose={onClose} />);

    await user.click(await screen.findByRole("button", { name: "Cerrar detalle" }));

    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
