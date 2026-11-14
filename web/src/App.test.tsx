import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import App from "./App";
import type { ApiClient, EventRow, Health, NodeDetail } from "./api/client";
import type { StreamLike } from "./useCommandRun";

const HEALTH: Health = {
  infra: { postgres: "down", kafka: "healthy", ollama: "healthy" },
  processes: { relay: true, planner: true },
  processes_up: 2,
  processes_total: 12,
};

function client(overrides: Partial<ApiClient> = {}): ApiClient {
  return {
    getHealth: vi.fn(async () => HEALTH),
    getConfig: vi.fn(async () => ({ jaeger_ui_url: "http://jaeger.local:16686" })),
    getTopology: vi.fn(async () => ({ nodes: [], edges: [] })),
    getEvents: vi.fn(async () => ({ events: [], next_before_seq: null })),
    getNode: vi.fn(async () => Promise.reject(new Error("sin detalle en este test"))),
    runCommand: vi.fn(),
    sendInput: vi.fn(),
    ...overrides,
  } as ApiClient;
}

class FakeStream implements StreamLike {
  static all: FakeStream[] = [];
  readonly listeners = new Map<string, (event: MessageEvent) => void>();
  closed = false;
  constructor(readonly url: string) {
    FakeStream.all.push(this);
  }
  addEventListener(type: string, listener: (event: MessageEvent) => void) {
    this.listeners.set(type, listener);
  }
  close() {
    this.closed = true;
  }
  emit(type: string, data: unknown) {
    act(() => this.listeners.get(type)?.(new MessageEvent(type, { data: JSON.stringify(data) })));
  }
}

const openStream = (url: string) => new FakeStream(url);

describe("Distribución de la pantalla", () => {
  it("Vista en reposo: menú, mapa, panel de eventos y ninguna ejecución", async () => {
    render(<App client={client()} openStream={openStream} />);

    expect(screen.getByRole("banner")).toHaveTextContent("EDA Retail");
    expect(screen.getByRole("complementary", { name: "Comandos" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Mapa de sistemas" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Eventos intercambiados" })).toBeInTheDocument();
    expect(screen.getByRole("log", { name: "Salida" })).toHaveTextContent("Sin ejecuciones todavía");
  });

  it("Salud de la infraestructura: la cabecera muestra cada servicio y los procesos", async () => {
    render(<App client={client()} openStream={openStream} />);

    expect(await screen.findByText("PostgreSQL: down")).toBeInTheDocument();
    expect(screen.getByText("Kafka: healthy")).toBeInTheDocument();
    expect(screen.getByText("Ollama: healthy")).toBeInTheDocument();
    expect(screen.getByText("Procesos: 2 de 12")).toBeInTheDocument();
  });

  it("muestra que el backend no responde si falla la consulta de salud", async () => {
    render(<App client={client({ getHealth: vi.fn(async () => Promise.reject(new Error("boom"))) })} openStream={openStream} />);

    expect(await screen.findByText("Backend: sin conexión")).toBeInTheDocument();
  });

  it("al pulsar un nodo del mapa queda seleccionado", async () => {
    const topology = {
      nodes: [{ id: "relay", label: "Relay", kind: "code" as const, description: "", monitored: true }],
      edges: [],
    };
    render(<App client={client({ getTopology: vi.fn(async () => topology) })} openStream={openStream} />);

    const relay = await screen.findByRole("button", { name: "Relay" });
    await userEvent.setup().click(relay);

    expect(relay).toHaveAttribute("aria-pressed", "true");
  });
});

describe("Panel de eventos en la pantalla", () => {
  it("carga la primera página de eventos y abre el stream en vivo", async () => {
    FakeStream.all = [];
    const api = client({
      getEvents: vi.fn(async () => ({
        events: [
          {
            seq: 1303,
            event_id: "ev-1303",
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
          },
        ],
        next_before_seq: null,
      })),
    });
    const { unmount } = render(<App client={api} openStream={openStream} />);

    expect(await screen.findByText("PlanCreated")).toBeInTheDocument();
    expect(api.getEvents).toHaveBeenCalledWith({ limit: 100, incidentId: undefined, node: undefined });
    expect(FakeStream.all.map((s) => s.url)).toContain("/api/events/stream?after_seq=1303");
    unmount();
    expect(FakeStream.all.every((s) => s.closed)).toBe(true);
  });

  it("Filtrar eventos por nodo: desde el mapa se abre el detalle y se filtra el panel", async () => {
    const topology = {
      nodes: [{ id: "planner", label: "Planificador", kind: "llm" as const, description: "", monitored: true }],
      edges: [],
    };
    const detail: NodeDetail = {
      id: "planner", label: "Planificador", kind: "llm", description: "Propone el plan.", monitored: true,
      up: true, consumes: ["ShipmentDelayed"], emits: ["PlanCreated"], llm: null,
    };
    const api = client({ getTopology: vi.fn(async () => topology), getNode: vi.fn(async () => detail) });
    const user = userEvent.setup();
    render(<App client={api} openStream={openStream} />);

    await user.click(await screen.findByRole("button", { name: "Planificador" }));
    await user.click(await screen.findByRole("button", { name: "Filtrar eventos" }));

    expect(api.getNode).toHaveBeenCalledWith("planner");
    expect(api.getEvents).toHaveBeenLastCalledWith(expect.objectContaining({ node: "planner" }));
  });

  it("Comunicación en curso: un evento en vivo resalta su camino en el mapa", async () => {
    FakeStream.all = [];
    const topology = {
      nodes: [
        { id: "orchestrator", label: "Orquestador", kind: "code" as const, description: "", monitored: true },
        { id: "postgres", label: "PostgreSQL", kind: "data" as const, description: "", monitored: false },
      ],
      edges: [{ source: "orchestrator", target: "postgres" }],
    };
    const { container } = render(
      <App client={client({ getTopology: vi.fn(async () => topology) })} openStream={openStream} />,
    );
    await screen.findByRole("button", { name: "Orquestador" });
    const events = FakeStream.all.find((stream) => stream.url.startsWith("/api/events/stream"));
    const row: EventRow = {
      seq: 7, event_id: "e7", event_type: "StepRequested", occurred_at: "2026-11-14T16:00:00+00:00",
      sender: "orchestrator", receiver: "postgres", receivers: ["postgres"], incident_id: "INC-1",
      plan_version: 1, partition: null, offset: null, causation_id: null, payload: {}, trace_id: null,
    };

    events!.emit("event", row);

    const active = Array.from(container.querySelectorAll("[data-edge].edge--active")).map((e) =>
      e.getAttribute("data-edge"),
    );
    expect(active).toEqual(["orchestrator->postgres"]);
  });

  it("Abrir Jaeger desde la cabecera: enlace a JAEGER_UI_URL en una pestaña nueva", async () => {
    render(<App client={client()} openStream={openStream} />);

    const link = within(screen.getByRole("banner")).getByRole("link", { name: "Jaeger" });

    await waitFor(() => expect(link).toHaveAttribute("href", "http://jaeger.local:16686"));
    expect(link).toHaveAttribute("target", "_blank");
    expect(link.getAttribute("rel")).toContain("noopener");
  });
});

describe("Reinicio de la vista", () => {
  it("Resetear la vista: panel vacío, detalle cerrado, salida en reposo y nada escrito en el backend", async () => {
    FakeStream.all = [];
    const topology = {
      nodes: [{ id: "planner", label: "Planificador", kind: "llm" as const, description: "", monitored: true }],
      edges: [],
    };
    const detail: NodeDetail = {
      id: "planner", label: "Planificador", kind: "llm", description: "Propone el plan.", monitored: true,
      up: true, consumes: ["ShipmentDelayed"], emits: ["PlanCreated"], llm: null,
    };
    const event: EventRow = {
      seq: 1290, event_id: "e1290", event_type: "PlanCreated", occurred_at: "2026-11-14T16:00:00+00:00",
      sender: "planner", receiver: "orchestrator", receivers: ["orchestrator"], incident_id: "INC-1",
      plan_version: 1, partition: null, offset: null, causation_id: null, payload: {}, trace_id: null,
    };
    const api = client({
      getTopology: vi.fn(async () => topology),
      getNode: vi.fn(async () => detail),
      getEvents: vi.fn(async () => ({ events: [event], next_before_seq: null })),
      runCommand: vi.fn(async () => ({ run_id: "run-1", argv: ["scripts/notice.sh", "seed"] })),
    });
    const user = userEvent.setup();
    render(<App client={api} openStream={openStream} />);
    await user.click(await screen.findByRole("button", { name: /^seed/ }));
    const run = FakeStream.all.find((stream) => stream.url === "/api/commands/run-1/stream")!;
    run.emit("output", { text: "datos cargados\n" });
    const reset = screen.getByRole("button", { name: "Resetear simulación" });
    expect(reset).toBeDisabled();
    run.emit("exit", { code: 0 });
    await user.click(await screen.findByRole("button", { name: "Planificador" }));
    expect(await screen.findByRole("button", { name: "Filtrar eventos" })).toBeInTheDocument();
    const panel = screen.getByRole("region", { name: "Eventos intercambiados" });
    expect(within(panel).getByText("PlanCreated")).toBeInTheDocument();
    const writes = (api.runCommand as ReturnType<typeof vi.fn>).mock.calls.length;

    await user.click(reset);

    expect(within(panel).queryByText("PlanCreated")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Filtrar eventos" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Planificador" })).toHaveAttribute("aria-pressed", "false");
    expect(screen.getByRole("log", { name: "Salida" })).toHaveTextContent("Sin ejecuciones todavía");
    expect(api.runCommand).toHaveBeenCalledTimes(writes);
    expect(api.sendInput).not.toHaveBeenCalled();
  });
});
