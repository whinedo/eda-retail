import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type {
  ApiClient,
  EventRow,
  Health,
  NodeDetail,
  OrderAnalytics,
  OrderStatus,
  OrderStatusRow,
  ParkedNotice,
} from "./api/client";
import { incidentRow, operationsData } from "./test/operationsFixtures";
import type { StreamLike } from "./useCommandRun";
import type { OperationsData } from "./useOperations";

const HEALTH: Health = {
  infra: { postgres: "down", kafka: "healthy", ollama: "healthy" },
  processes: { relay: true, planner: true },
  processes_up: 2,
  processes_total: 12,
};

const EMPTY_COUNTS: Record<OrderStatus, number> = { delayed: 0, in_resolution: 0, resolved: 0, refunded: 0, escalated: 0 };

function orderRow(order_id: string, status: OrderStatus, last_event_type: string): OrderStatusRow {
  return {
    order_id,
    incident_id: "INC-0007",
    status,
    last_event_type,
    last_event_id: "d282ef26-1a2b-4c3d-9e8f-001122334455",
    occurred_at: "2026-11-14T01:48:19.234567+00:00",
    kafka_partition: 0,
    kafka_offset: 7,
  };
}

function analytics(rows: OrderStatusRow[]): OrderAnalytics {
  const counts = { ...EMPTY_COUNTS };
  for (const row of rows) counts[row.status] += 1;
  return { counts, orders: rows, applied: { "0": 7 } };
}

function client(overrides: Partial<ApiClient> = {}): ApiClient {
  return {
    getHealth: vi.fn(async () => HEALTH),
    getConfig: vi.fn(async () => ({ jaeger_ui_url: "http://jaeger.local:16686", free_text_enabled: false, interactive_enabled: false })),
    getTopology: vi.fn(async () => ({ nodes: [], edges: [] })),
    getEvents: vi.fn(async () => ({ events: [], next_before_seq: null })),
    getNode: vi.fn(async () => Promise.reject(new Error("sin detalle en este test"))),
    getOrderAnalytics: vi.fn(async () => ({ counts: EMPTY_COUNTS, orders: [], applied: {} })),
    getParkedNotices: vi.fn(async () => ({ notices: [], applied: {} })),
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

  it("Reinicio con un comando en ejecución", async () => {
    FakeStream.all = [];
    const api = client({
      runCommand: vi.fn(async () => ({ run_id: "run-2", argv: ["scripts/notice.sh", "seed"] })),
    });
    const user = userEvent.setup();
    render(<App client={api} openStream={openStream} />);
    const reset = screen.getByRole("button", { name: "Resetear simulación" });
    expect(reset).toBeEnabled();

    // GIVEN un comando en ejecución (lanzado y sin `exit` todavía)
    await user.click(await screen.findByRole("button", { name: /^seed/ }));
    await waitFor(() => expect(FakeStream.all.some((stream) => stream.url === "/api/commands/run-2/stream")).toBe(true));

    // WHEN el usuario mira la cabecera / THEN «Resetear simulación» está desactivado
    const header = screen.getByRole("banner");
    expect(within(header).getByRole("button", { name: "Resetear simulación" })).toBeDisabled();
  });
});

describe("Altura del panel de eventos", () => {
  const height = () => screen.getByRole("main").style.getPropertyValue("--events-height");

  it("Reducir el panel para ver el mapa: arrastrar el separador 200 px abajo lo deja en 140 px, nunca menos de 96", () => {
    window.localStorage.clear();
    render(<App client={client()} openStream={openStream} />);
    const separator = screen.getByRole("separator", { name: "Altura del panel de eventos" });
    expect(separator).toHaveAttribute("aria-orientation", "horizontal");
    expect(height()).toBe("340px");

    fireEvent.mouseDown(separator, { clientY: 500 });
    fireEvent.mouseMove(window, { clientY: 700 });
    expect(height()).toBe("140px");
    expect(separator).toHaveAttribute("aria-valuenow", "140");
    fireEvent.mouseMove(window, { clientY: 900 });
    fireEvent.mouseUp(window);
    expect(height()).toBe("96px");

    fireEvent.mouseMove(window, { clientY: 100 });
    expect(height()).toBe("96px"); // tras soltar, mover el ratón no cambia nada
  });

  it("con el teclado: flechas de 20 px, Inicio al mínimo, y se recuerda al volver a abrir", () => {
    window.localStorage.clear();
    const { unmount } = render(<App client={client()} openStream={openStream} />);
    const separator = screen.getByRole("separator", { name: "Altura del panel de eventos" });

    fireEvent.keyDown(separator, { key: "ArrowDown" });
    expect(height()).toBe("320px");
    fireEvent.keyDown(separator, { key: "ArrowUp" });
    fireEvent.keyDown(separator, { key: "ArrowUp" });
    expect(height()).toBe("360px");
    fireEvent.keyDown(separator, { key: "Home" });
    expect(height()).toBe("96px");
    unmount();

    render(<App client={client()} openStream={openStream} />);
    expect(height()).toBe("96px");
  });

  it("no deja al mapa menos de 160 px", () => {
    window.localStorage.clear();
    render(<App client={client()} openStream={openStream} />);
    const separator = screen.getByRole("separator", { name: "Altura del panel de eventos" });

    fireEvent.keyDown(separator, { key: "End" });

    // jsdom: ventana de 768 px; cabecera 56 px y separador 6 px → 768 − 56 − 6 − 160.
    expect(height()).toBe("546px");
    expect(separator).toHaveAttribute("aria-valuemax", "546");
  });
});

describe("Texto libre según /api/config", () => {
  it("Texto libre deshabilitado: sin free_text_enabled no aparece send --text", async () => {
    const api = client();
    render(<App client={api} openStream={openStream} />);

    await waitFor(() => expect(api.getConfig).toHaveBeenCalled());
    expect(screen.queryByRole("textbox", { name: "Texto libre del aviso" })).not.toBeInTheDocument();
  });

  it("con WEB_FREE_TEXT=true el backend lo habilita y aparece send --text", async () => {
    const api = client({
      getConfig: vi.fn(async () => ({ jaeger_ui_url: "http://jaeger.local:16686", free_text_enabled: true, interactive_enabled: false })),
    });
    render(<App client={api} openStream={openStream} />);

    expect(await screen.findByRole("textbox", { name: "Texto libre del aviso" })).toBeInTheDocument();
  });
});

describe("Versión de la PoC según /api/config", () => {
  it("con POC_VERSION=2 la cabecera muestra «Versión 2 (MCP + A2A)»", async () => {
    const api = client({
      getConfig: vi.fn(async () => ({
        jaeger_ui_url: "",
        free_text_enabled: false,
        interactive_enabled: false,
        poc_version: 2,
      })),
    });
    render(<App client={api} openStream={openStream} />);

    expect(await screen.findByText("Versión 2 (MCP + A2A)")).toBeInTheDocument();
  });

  it("con POC_VERSION=1 la cabecera muestra «Versión 1»", async () => {
    const api = client({
      getConfig: vi.fn(async () => ({
        jaeger_ui_url: "",
        free_text_enabled: false,
        interactive_enabled: false,
        poc_version: 1,
      })),
    });
    render(<App client={api} openStream={openStream} />);

    expect(await screen.findByText("Versión 1")).toBeInTheDocument();
  });
});

describe("Interactive según /api/config", () => {
  it("Interactive deshabilitado: sin interactive_enabled no aparece interactive", async () => {
    const api = client();
    render(<App client={api} openStream={openStream} />);

    await waitFor(() => expect(api.getConfig).toHaveBeenCalled());
    expect(screen.queryByRole("button", { name: /^interactive/ })).not.toBeInTheDocument();
  });

  it("con WEB_INTERACTIVE=true el backend lo habilita y aparece interactive", async () => {
    const api = client({
      getConfig: vi.fn(async () => ({ jaeger_ui_url: "", free_text_enabled: false, interactive_enabled: true })),
    });
    render(<App client={api} openStream={openStream} />);

    expect(await screen.findByRole("button", { name: /^interactive/ })).toBeInTheDocument();
  });
});

describe("Vista de analítica de pedidos en la pantalla", () => {
  beforeEach(() => {
    window.location.hash = "#/";
  });

  it("Acceso desde la vista principal: contadores y tabla, y vuelta sin perder el filtro ni la altura", async () => {
    window.localStorage.clear();
    const user = userEvent.setup();
    const api = client({ getOrderAnalytics: vi.fn(async () => analytics([orderRow("P-1001", "delayed", "ShipmentDelayed")])) });
    render(<App client={api} openStream={openStream} />);
    fireEvent.keyDown(screen.getByRole("separator", { name: "Altura del panel de eventos" }), { key: "ArrowDown" });
    const filter = within(screen.getByRole("region", { name: "Eventos intercambiados" })).getByLabelText("Incidencia");
    await user.type(filter, "INC-0007{Enter}");

    await user.click(screen.getByRole("link", { name: "Analítica" }));

    expect(window.location.hash).toBe("#/analytics");
    const view = screen.getByRole("region", { name: "Estado de los pedidos" });
    expect(within(view).getByRole("list", { name: "Pedidos por estado" })).toBeInTheDocument();
    expect(await within(view).findByRole("row", { name: /P-1001/ })).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "Eventos intercambiados" })).not.toBeInTheDocument();
    expect(screen.getByRole("banner")).toHaveTextContent("Kafka");

    await user.click(screen.getByRole("link", { name: "← Vista principal" }));

    expect(screen.queryByRole("region", { name: "Estado de los pedidos" })).not.toBeInTheDocument();
    const events = screen.getByRole("region", { name: "Eventos intercambiados" });
    expect(within(events).getByLabelText("Incidencia")).toHaveValue("INC-0007");
    expect(screen.getByRole("main").style.getPropertyValue("--events-height")).toBe("320px");
  });

  it("Pedido reembolsado en vivo: en menos de 2 segundos aparece como Reembolsada y cambian los contadores", async () => {
    window.location.hash = "#/analytics";
    let current = analytics([orderRow("P-1001", "in_resolution", "PlanCreated")]);
    const api = client({ getOrderAnalytics: vi.fn(async () => current) });
    render(<App client={api} openStream={openStream} />);
    const view = screen.getByRole("region", { name: "Estado de los pedidos" });
    expect(await within(view).findByText("En proceso de solución", { selector: "td" })).toBeInTheDocument();

    current = analytics([orderRow("P-1001", "refunded", "IncidentResolved")]);

    expect(await within(view).findByText("Reembolsada", { selector: "td" }, { timeout: 2000 })).toBeInTheDocument();
    expect(within(view).getByRole("button", { name: /Reembolsada/ })).toHaveTextContent("1Reembolsada");
    expect(within(view).getByRole("button", { name: /En proceso de solución/ })).toHaveTextContent("0En proceso de solución");
  });

  it("Filtro por estado: pulsar «Envío retrasado» pide y muestra solo los pedidos en delayed", async () => {
    window.location.hash = "#/analytics";
    const rows = [orderRow("P-1001", "delayed", "ShipmentDelayed"), orderRow("P-1002", "refunded", "IncidentResolved")];
    const api = client({
      getOrderAnalytics: vi.fn(async (query = {}) => ({
        ...analytics(rows),
        orders: rows.filter((r) => !query.status || r.status === query.status),
      })),
    });
    const user = userEvent.setup();
    render(<App client={api} openStream={openStream} />);
    const view = screen.getByRole("region", { name: "Estado de los pedidos" });
    expect(await within(view).findByRole("row", { name: /P-1002/ })).toBeInTheDocument();

    await user.click(within(view).getByRole("button", { name: /Envío retrasado/ }));

    await waitFor(() => expect(within(view).queryByRole("row", { name: /P-1002/ })).not.toBeInTheDocument());
    expect(within(view).getByRole("row", { name: /P-1001/ })).toBeInTheDocument();
    expect(api.getOrderAnalytics).toHaveBeenLastCalledWith({ status: "delayed", incidentId: undefined });
  });

  it("no consulta la analítica mientras está abierta la vista principal", async () => {
    const api = client();
    render(<App client={api} openStream={openStream} />);
    await screen.findByText("Kafka: healthy");
    expect(api.getOrderAnalytics).not.toHaveBeenCalled();
  });
});

describe("Aviso de avisos aparcados en la pantalla", () => {
  beforeEach(() => {
    window.location.hash = "#/";
  });

  const parked = (incident_id: string, order_ids: string[], deadline_at: string): ParkedNotice => ({
    incident_id,
    order_ids,
    deadline_at,
    parked_at: "2026-11-15T20:08:36+00:00",
    last_event_id: "d282ef26-1a2b-4c3d-9e8f-001122334455",
    kafka_partition: 0,
    kafka_offset: 3,
  });

  it("Aviso de avisos aparcados: visible en la vista principal y en la analítica mientras haya alguno", async () => {
    let notices = [parked("I-3f2a9c1b-0000-5000-8000-000000000000", ["P-1001"], "2026-11-15T20:13:36+00:00")];
    const api = client({ getParkedNotices: vi.fn(async () => ({ notices, applied: { "0": 3 } })) });
    const user = userEvent.setup();
    render(<App client={api} openStream={openStream} />);

    const banner = await screen.findByRole("status", { name: "Avisos aparcados" });
    expect(banner).toHaveTextContent("1 aviso aparcado espera pedidos que aún no existen");
    expect(banner).toHaveTextContent("P-1001");
    expect(banner).toHaveTextContent("I-3f2a9c1b");
    expect(banner).toHaveTextContent("usa «seed»");

    await user.click(screen.getByRole("link", { name: "Analítica" }));
    expect(screen.getByRole("region", { name: "Estado de los pedidos" })).toBeInTheDocument();
    expect(screen.getByRole("status", { name: "Avisos aparcados" })).toHaveTextContent("P-1001");

    notices = [];
    await waitFor(() => expect(screen.queryByRole("status", { name: "Avisos aparcados" })).not.toBeInTheDocument(), {
      timeout: 2000,
    });
  });

  it("sin avisos aparcados no se muestra el aviso", async () => {
    const api = client();
    render(<App client={api} openStream={openStream} />);
    await waitFor(() => expect(api.getParkedNotices).toHaveBeenCalled());
    expect(screen.queryByRole("status", { name: "Avisos aparcados" })).not.toBeInTheDocument();
  });
});

// Los métodos de /api/operations/* del cliente falso a partir de los datos de la vista.
function operationsClient(get: () => OperationsData): Partial<ApiClient> {
  return {
    getOperationsSummary: vi.fn(async () => get().summary),
    getOperationsIncidents: vi.fn(async () => ({ incidents: get().incidents, applied: {}, next_before: get().nextBefore })),
    getOperationsLlm: vi.fn(async () => get().llm),
    getOperationsCarriers: vi.fn(async () => get().carriers),
    getOperationsFreshness: vi.fn(async () => get().freshness),
  };
}

describe("Vista de operaciones en la pantalla", () => {
  beforeEach(() => {
    window.location.hash = "#/";
  });

  it("Entrada por la cabecera: Analítica abre pedidos con el enlace «Operaciones» junto al título y no en la cabecera", async () => {
    const user = userEvent.setup();
    render(<App client={client(operationsClient(() => operationsData()))} openStream={openStream} />);
    await user.click(screen.getByRole("link", { name: "Analítica" }));
    const orders = screen.getByRole("region", { name: "Estado de los pedidos" });
    expect(within(orders).getByRole("link", { name: "Operaciones" })).toBeInTheDocument();
    expect(within(screen.getByRole("banner")).queryByRole("link", { name: "Operaciones" })).not.toBeInTheDocument();
  });

  it("De pedidos a operaciones y vuelta: vuelve a la vista de pedidos sin perder su filtro", async () => {
    window.location.hash = "#/analytics";
    const user = userEvent.setup();
    const api = client(operationsClient(() => operationsData()));
    render(<App client={api} openStream={openStream} />);
    const orders = screen.getByRole("region", { name: "Estado de los pedidos" });
    await user.type(within(orders).getByLabelText("Incidencia"), "INC-0007{Enter}");

    await user.click(within(orders).getByRole("link", { name: "Operaciones" }));
    expect(window.location.hash).toBe("#/analytics/operations");
    expect(await screen.findByRole("region", { name: "Operaciones" })).toBeInTheDocument();
    expect(within(screen.getByRole("banner")).getByRole("link", { name: "Analítica" })).toHaveAttribute("aria-current", "page");

    await user.click(screen.getByRole("link", { name: "← Estado de los pedidos" }));
    const back = screen.getByRole("region", { name: "Estado de los pedidos" });
    expect(within(back).getByLabelText("Incidencia")).toHaveValue("INC-0007");
  });

  it("Volver a la vista principal: ve el mapa y el panel de eventos sin perder la altura del panel", async () => {
    window.localStorage.clear();
    const user = userEvent.setup();
    render(<App client={client(operationsClient(() => operationsData()))} openStream={openStream} />);
    fireEvent.keyDown(screen.getByRole("separator", { name: "Altura del panel de eventos" }), { key: "ArrowDown" });
    act(() => {
      window.location.hash = "#/analytics/operations";
      window.dispatchEvent(new HashChangeEvent("hashchange"));
    });
    await user.click(await screen.findByRole("link", { name: "← Vista principal" }));
    expect(screen.getByRole("region", { name: "Eventos intercambiados" })).toBeInTheDocument();
    expect(screen.getByRole("main").style.getPropertyValue("--events-height")).toBe("320px");
  });

  it("Dirección directa: cargar #/analytics/operations muestra la vista de operaciones", async () => {
    window.location.hash = "#/analytics/operations";
    render(<App client={client(operationsClient(() => operationsData()))} openStream={openStream} />);
    const view = screen.getByRole("region", { name: "Operaciones" });
    expect(await within(view).findByRole("row", { name: /I-1/ })).toBeInTheDocument();
  });

  it("Ver los eventos de una incidencia: abre la vista principal con su incident_id en el filtro Incidencia", async () => {
    window.location.hash = "#/analytics/operations";
    const user = userEvent.setup();
    render(<App client={client(operationsClient(() => operationsData()))} openStream={openStream} />);
    await user.click(await screen.findByRole("button", { name: "Ver eventos" }));
    expect(window.location.hash).toBe("#/");
    const events = screen.getByRole("region", { name: "Eventos intercambiados" });
    expect(within(events).getByLabelText("Incidencia")).toHaveValue("I-1");
  });

  it("Incidencia que se cierra en vivo: en menos de 2 segundos pasa a Escalada y aparece la barra step_timeout", async () => {
    window.location.hash = "#/analytics/operations";
    let current = operationsData({ incidents: [incidentRow("I-9", { state: "open", closed_at: null, duration_s: null })] });
    current.summary = { ...current.summary, by_state: { open: 1, resolved: 0, refunded: 0, escalated: 0 }, escalations: [] };
    render(<App client={client(operationsClient(() => current))} openStream={openStream} />);
    const view = screen.getByRole("region", { name: "Operaciones" });
    expect(await within(view).findByText("Abierta", { selector: "td" })).toBeInTheDocument();

    current = operationsData({ incidents: [incidentRow("I-9", { state: "escalated", escalation_reason: "step_timeout" })] });
    current.summary = { ...current.summary, by_state: { open: 0, resolved: 0, refunded: 0, escalated: 1 },
      escalations: [{ reason: "step_timeout", count: 1 }] };

    expect(await within(view).findByText("Escalada", { selector: "td" }, { timeout: 2000 })).toBeInTheDocument();
    expect(within(view).getByRole("button", { name: /^1Escalada/ })).toBeInTheDocument();
    expect(within(view).getByRole("button", { name: /step_timeout/ })).toBeInTheDocument();
  });

  it("Vista en pausa: la pantalla no cambia y al reanudar aparecen los cambios", async () => {
    window.location.hash = "#/analytics/operations";
    const user = userEvent.setup();
    let current = operationsData({ incidents: [incidentRow("I-9", { state: "open", closed_at: null, duration_s: null })] });
    render(<App client={client(operationsClient(() => current))} openStream={openStream} />);
    const view = screen.getByRole("region", { name: "Operaciones" });
    expect(await within(view).findByText("Abierta", { selector: "td" })).toBeInTheDocument();
    await user.click(within(view).getByRole("button", { name: "Pausar" }));
    expect(within(view).getByText("en pausa")).toBeInTheDocument();

    current = operationsData({ incidents: [incidentRow("I-9", { state: "resolved" })] });
    await new Promise((r) => setTimeout(r, 1500));
    expect(within(view).getByText("Abierta", { selector: "td" })).toBeInTheDocument();

    await user.click(within(view).getByRole("button", { name: "Reanudar" }));
    expect(await within(view).findByText("Solucionada", { selector: "td" })).toBeInTheDocument();
  });

  it("Resetear simulación: la vista de operaciones conserva sus datos", async () => {
    window.location.hash = "#/analytics/operations";
    const user = userEvent.setup();
    render(<App client={client(operationsClient(() => operationsData()))} openStream={openStream} />);
    const view = screen.getByRole("region", { name: "Operaciones" });
    expect(await within(view).findByRole("row", { name: /I-1/ })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Resetear simulación" }));
    expect(within(screen.getByRole("region", { name: "Operaciones" })).getByRole("row", { name: /I-1/ })).toBeInTheDocument();
  });

  it("Fallo de carga: error en rojo y la vista se recupera sola cuando la consulta vuelve a responder", async () => {
    window.location.hash = "#/analytics/operations";
    let fail = true;
    const api = client({
      ...operationsClient(() => operationsData()),
      getOperationsSummary: vi.fn(async () => {
        if (fail) throw new Error("HTTP 500");
        return operationsData().summary;
      }),
    });
    render(<App client={api} openStream={openStream} />);
    expect(await screen.findByText("HTTP 500")).toHaveClass("events__error");
    fail = false;
    await waitFor(() => expect(screen.queryByText("HTTP 500")).not.toBeInTheDocument(), { timeout: 2000 });
    expect(screen.getByRole("row", { name: /I-1/ })).toBeInTheDocument();
  });
});
