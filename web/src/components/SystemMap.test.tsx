import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ApiClient, Health, Topology } from "../api/client";
import type { HealthState } from "../useHealth";
import { HIGHLIGHT_MS, SystemMap, type Highlight } from "./SystemMap";
import { SystemMapPanel } from "./SystemMapPanel";

const EXECUTORS = ["notify_customer", "file_claim", "reserve_stock", "reissue_shipment", "release_stock", "issue_refund"];
const CONSUMERS = ["interpreter", "planner", "replanner", "orchestrator", ...EXECUTORS.map((a) => `executor-${a}`)];

// Réplica reducida de src/eda_retail/web/topology.py.
const TOPOLOGY: Topology = {
  nodes: [
    { id: "intake", label: "eda-notice", kind: "code", description: "CLI de avisos", monitored: false },
    { id: "postgres", label: "PostgreSQL", kind: "data", description: "Log de eventos", monitored: false },
    { id: "relay", label: "Relay", kind: "code", description: "Publica el outbox", monitored: true },
    { id: "kafka", label: "Kafka (eda.events)", kind: "data", description: "Topic único", monitored: false },
    { id: "dlq", label: "eda.dlq", kind: "data", description: "Dead letters", monitored: false },
    { id: "projectors", label: "Proyector", kind: "code", description: "Vistas", monitored: true },
    { id: "interpreter", label: "Intérprete", kind: "llm", description: "Agente", monitored: true },
    { id: "planner", label: "Planificador", kind: "llm", description: "Agente", monitored: true },
    { id: "replanner", label: "Replanificador", kind: "llm", description: "Agente", monitored: true },
    { id: "orchestrator", label: "Orquestador", kind: "code", description: "Plan", monitored: true },
    ...EXECUTORS.map((a) => ({
      id: `executor-${a}`,
      label: a,
      kind: "code" as const,
      description: `Ejecutor de ${a}`,
      monitored: true,
    })),
    { id: "ollama", label: "Ollama", kind: "external", description: "LLM local", monitored: false },
    { id: "mail", label: "Correo", kind: "simulated", description: "Correo simulado", monitored: false },
    { id: "jaeger", label: "Jaeger", kind: "external", description: "Trazas", monitored: false },
  ],
  edges: [
    { source: "intake", target: "postgres" },
    { source: "postgres", target: "relay" },
    { source: "relay", target: "kafka" },
    { source: "relay", target: "dlq" },
    { source: "postgres", target: "projectors" },
    ...CONSUMERS.map((c) => ({ source: "kafka", target: c })),
    ...CONSUMERS.map((c) => ({ source: c, target: "postgres" })),
    { source: "planner", target: "ollama" },
    { source: "executor-notify_customer", target: "mail" },
  ],
};

const HEALTH: Health = {
  infra: { postgres: "healthy", kafka: "healthy", ollama: "healthy" },
  processes: { relay: true, planner: false },
  processes_up: 1,
  processes_total: 12,
};

const OK: HealthState = { kind: "ok", health: HEALTH };

function renderMap(props: Partial<Parameters<typeof SystemMap>[0]> = {}) {
  const onSelect = vi.fn();
  const view = render(
    <SystemMap topology={TOPOLOGY} health={OK} highlight={null} selected={null} onSelect={onSelect} {...props} />,
  );
  return { onSelect, ...view };
}

const node = (label: string) => screen.getByRole("button", { name: label });
const activeEdges = (container: HTMLElement) =>
  Array.from(container.querySelectorAll("[data-edge].edge--active")).map((e) => e.getAttribute("data-edge"));

afterEach(() => vi.useRealTimers());

describe("Mapa de sistemas", () => {
  it("pinta cada nodo de la topología con su etiqueta y su tipo", () => {
    const { container } = renderMap();

    for (const n of TOPOLOGY.nodes) {
      expect(node(n.label)).toHaveClass(`node--${n.kind}`);
    }
    expect(container.querySelectorAll("[data-edge]")).toHaveLength(TOPOLOGY.edges.length);
  });

  it("un nodo que no está en la tabla de posiciones también se pinta", () => {
    renderMap({
      topology: {
        nodes: [...TOPOLOGY.nodes, { id: "nuevo", label: "Nodo nuevo", kind: "code", description: "", monitored: false }],
        edges: [...TOPOLOGY.edges, { source: "kafka", target: "nuevo" }],
      },
    });

    expect(node("Nodo nuevo")).toBeInTheDocument();
  });

  it("Estado de un nodo: activo si el proceso existe y caído si no", () => {
    renderMap();

    expect(node("Relay")).toHaveAccessibleDescription("Proceso activo");
    expect(node("Planificador")).toHaveAccessibleDescription("Proceso caído");
    // Vigilado pero sin dato en la respuesta de salud.
    expect(node("Orquestador")).toHaveAccessibleDescription("Estado desconocido");
    // Sin proceso propio: sin indicador.
    expect(node("PostgreSQL")).not.toHaveAccessibleDescription();
  });

  it("el estado es desconocido mientras carga la salud o sin conexión", () => {
    const { rerender, onSelect } = renderMap({ health: { kind: "loading" } });
    expect(node("Relay")).toHaveAccessibleDescription("Estado desconocido");

    rerender(
      <SystemMap topology={TOPOLOGY} health={{ kind: "offline" }} highlight={null} selected={null} onSelect={onSelect} />,
    );
    expect(node("Relay")).toHaveAccessibleDescription("Estado desconocido");
  });

  it("muestra la leyenda de tipos de nodo", () => {
    renderMap();

    const legend = screen.getByRole("list", { name: "Leyenda" });
    for (const text of ["Agente con LLM", "Código", "Datos y bus", "Entrada o externo", "Servicio simulado"]) {
      expect(within(legend).getByText(text)).toBeInTheDocument();
    }
  });
});

describe("Comunicación en curso", () => {
  it("resalta el camino entre emisor y destino unos segundos", () => {
    vi.useFakeTimers();
    const highlight: Highlight = { sender: "orchestrator", receiver: "executor-issue_refund", seq: 7 };
    const { container } = renderMap({ highlight });

    expect(activeEdges(container).sort()).toEqual(
      ["orchestrator->postgres", "postgres->relay", "relay->kafka", "kafka->executor-issue_refund"].sort(),
    );
    expect(node("Orquestador")).toHaveClass("node--active");
    expect(node("issue_refund")).toHaveClass("node--active");
    expect(node("Relay")).not.toHaveClass("node--active");

    act(() => vi.advanceTimersByTime(HIGHLIGHT_MS - 1));
    expect(activeEdges(container)).toHaveLength(4);

    act(() => vi.advanceTimersByTime(1));
    expect(activeEdges(container)).toEqual([]);
    expect(node("Orquestador")).not.toHaveClass("node--active");
  });

  it("sin camino dirigido, usa las aristas sin dirección", () => {
    const { container } = renderMap({ highlight: { sender: "dlq", receiver: "relay", seq: 1 } });

    expect(activeEdges(container)).toEqual(["relay->dlq"]);
  });

  it("un evento nuevo reinicia el resaltado", () => {
    vi.useFakeTimers();
    const { container, rerender, onSelect } = renderMap({
      highlight: { sender: "intake", receiver: "postgres", seq: 1 },
    });
    act(() => vi.advanceTimersByTime(HIGHLIGHT_MS - 500));

    rerender(
      <SystemMap
        topology={TOPOLOGY}
        health={OK}
        highlight={{ sender: "planner", receiver: "ollama", seq: 2 }}
        selected={null}
        onSelect={onSelect}
      />,
    );
    expect(activeEdges(container)).toEqual(["planner->ollama"]);

    act(() => vi.advanceTimersByTime(1000));
    expect(activeEdges(container)).toEqual(["planner->ollama"]);
  });

  it("ignora emisores o destinos desconocidos", () => {
    const { container } = renderMap({ highlight: { sender: "nadie", receiver: "postgres", seq: 3 } });

    expect(activeEdges(container)).toEqual([]);
    expect(node("PostgreSQL")).not.toHaveClass("node--active");
  });
});

describe("Selección de un nodo", () => {
  it("al pulsar un nodo avisa con su id", async () => {
    const { onSelect } = renderMap();

    await userEvent.setup().click(node("Planificador"));

    expect(onSelect).toHaveBeenCalledWith("planner");
  });

  it("se puede seleccionar con el teclado", async () => {
    const { onSelect } = renderMap();
    const user = userEvent.setup();

    node("Kafka (eda.events)").focus();
    await user.keyboard("{Enter}");

    expect(onSelect).toHaveBeenCalledWith("kafka");
  });

  it("marca el nodo seleccionado", () => {
    renderMap({ selected: "relay" });

    expect(node("Relay")).toHaveAttribute("aria-pressed", "true");
    expect(node("Relay")).toHaveClass("node--selected");
    expect(node("Kafka (eda.events)")).toHaveAttribute("aria-pressed", "false");
  });
});

describe("Carga de la topología", () => {
  function client(getTopology: ApiClient["getTopology"]): ApiClient {
    return { getTopology } as unknown as ApiClient;
  }

  it("carga la topología una vez y pinta el mapa", async () => {
    const getTopology = vi.fn(async () => TOPOLOGY);
    render(
      <SystemMapPanel client={client(getTopology)} health={OK} highlight={null} selected={null} onSelect={vi.fn()} />,
    );

    expect(screen.getByRole("region", { name: "Mapa de sistemas" })).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Relay" })).toBeInTheDocument();
    expect(getTopology).toHaveBeenCalledTimes(1);
  });

  it("si falla la carga muestra un aviso breve", async () => {
    const getTopology = vi.fn(async () => Promise.reject(new Error("boom")));
    render(
      <SystemMapPanel client={client(getTopology)} health={OK} highlight={null} selected={null} onSelect={vi.fn()} />,
    );

    expect(await screen.findByText("No se pudo cargar la topología.")).toBeInTheDocument();
    expect(screen.queryAllByRole("button")).toHaveLength(0);
  });
});
