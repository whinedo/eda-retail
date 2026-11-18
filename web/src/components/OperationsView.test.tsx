import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { incidentRow, operationsData } from "../test/operationsFixtures";
import { OperationsView, type OperationsViewProps, formatDuration } from "./OperationsView";

function setup(props: Partial<OperationsViewProps> = {}) {
  const handlers = {
    onStateChange: vi.fn(),
    onReasonChange: vi.fn(),
    onIncidentChange: vi.fn(),
    onTogglePause: vi.fn(),
    onLoadMore: vi.fn(),
    onViewEvents: vi.fn(),
    onOrders: vi.fn(),
    onProfit: vi.fn(),
    onBack: vi.fn(),
  };
  render(
    <OperationsView
      data={operationsData()}
      error={null}
      state={null}
      reason={null}
      incidentId=""
      paused={false}
      {...handlers}
      {...props}
    />,
  );
  return { ...handlers, user: userEvent.setup() };
}

const view = () => screen.getByRole("region", { name: "Operaciones" });
const section = (name: string) => within(view()).getByRole("region", { name });
const rowCells = (scope: HTMLElement, name: RegExp) =>
  within(within(scope).getByRole("row", { name })).getAllByRole("cell").map((c) => c.textContent);

describe("Vista de operaciones", () => {
  it("Escaladas por motivo: contadores de Solucionada y Escalada y una barra con 7 (100 %)", () => {
    setup();
    const counters = within(view()).getByRole("list", { name: "Incidencias por estado" });
    expect(within(counters).getByRole("button", { name: /Solucionada/ })).toHaveTextContent("23Solucionada");
    expect(within(counters).getByRole("button", { name: /Escalada/ })).toHaveTextContent("7Escalada");
    const bars = within(view()).getByRole("group", { name: "Escaladas por motivo" });
    expect(within(bars).getByRole("button", { name: /llm_output_invalid/ })).toHaveTextContent("7 (100 %)");
  });

  it("Filtrar por motivo: pulsar la barra step_timeout filtra la tabla por ese motivo, y pulsarla otra vez lo quita", async () => {
    const data = operationsData();
    data.summary.escalations = [
      { reason: "llm_output_invalid", count: 5 },
      { reason: "step_timeout", count: 2 },
    ];
    const { user, onReasonChange } = setup({ data });
    await user.click(screen.getByRole("button", { name: /step_timeout/ }));
    expect(onReasonChange).toHaveBeenCalledWith("step_timeout");
    onReasonChange.mockClear();
    const filtered = setup({ data, reason: "step_timeout" });
    await filtered.user.click(screen.getAllByRole("button", { name: /step_timeout/ }).at(-1) as HTMLElement);
    expect(filtered.onReasonChange).toHaveBeenCalledWith(null);
  });

  it("Pulsar un contador filtra la tabla por estado", async () => {
    const { user, onStateChange } = setup();
    await user.click(screen.getByRole("button", { name: /Escalada/ }));
    expect(onStateChange).toHaveBeenCalledWith("escalated");
  });

  it("Sin escaladas: el contador Escalada vale 0 y el desglose dice «Sin incidencias escaladas»", () => {
    const data = operationsData();
    data.summary.by_state.escalated = 0;
    data.summary.escalations = [];
    setup({ data });
    expect(screen.getByRole("button", { name: /Escalada/ })).toHaveTextContent("0Escalada");
    expect(screen.getByText("Sin incidencias escaladas")).toBeInTheDocument();
  });

  it("Motivo desconocido: aparece una barra con su texto y la vista no falla", () => {
    const data = operationsData();
    data.summary.escalations = [{ reason: "algo_nuevo", count: 1 }];
    setup({ data });
    expect(screen.getByRole("button", { name: /algo_nuevo/ })).toHaveTextContent("1 (100 %)");
  });

  it("Incidencia con una revisión: estado Reembolsada, versión 2, 1 paso fallido y su duración", () => {
    setup();
    const cells = rowCells(within(view()).getByRole("group", { name: "Tabla de incidencias" }), /I-1/);
    expect(cells.slice(0, 9)).toEqual(["I-1", "Reembolsada", "", "SEUR", "loss / high", "2", "1", "0", "12,5 s"]);
    expect(cells[10]).toBe("450,00 €");
    expect(cells[11]).toBe("IncidentResolved · p1 / 130");
  });

  it("Incidencia abierta: su estado es Abierta y su duración está vacía", () => {
    setup({ data: operationsData({ incidents: [incidentRow("I-2", { state: "open", closed_at: null, duration_s: null })] }) });
    const cells = rowCells(view(), /I-2/);
    expect(cells[1]).toBe("Abierta");
    expect(cells[8]).toBe("");
  });

  it("Ver los eventos de una incidencia: pulsa «Ver eventos» con su incident_id", async () => {
    const { user, onViewEvents } = setup();
    await user.click(screen.getByRole("button", { name: "Ver eventos" }));
    expect(onViewEvents).toHaveBeenCalledWith("I-1");
  });

  it("Filtro por incidencia: escribir un incident_id y pulsar Intro lo aplica", async () => {
    const { user, onIncidentChange } = setup();
    await user.type(within(view()).getByLabelText("Incidencia"), "I-7{Enter}");
    expect(onIncidentChange).toHaveBeenCalledWith("I-7");
  });

  it("Desplazamiento lateral en operaciones: tabla con scroll y cabecera fija, y «Cargar más» si hay más páginas", async () => {
    const { user, onLoadMore } = setup({ data: operationsData({ nextBefore: "2026-11-17T10:00:00+00:00|I-1" }) });
    const table = within(view()).getByRole("group", { name: "Tabla de incidencias" });
    expect(table).toHaveClass("events__scroll");
    expect(within(table).getByRole("table").querySelector("thead")).toHaveClass("events__head");
    await user.click(screen.getByRole("button", { name: "Cargar más" }));
    expect(onLoadMore).toHaveBeenCalled();
  });

  it("Una pérdida cuesta más que un retraso leve, y sin precios configurados se avisa con el nombre de las variables", () => {
    setup();
    const cost = section("Coste por escenario");
    expect(rowCells(cost, /loss \/ high/)[2]).toBe("500");
    expect(rowCells(cost, /delay \/ low/)[2]).toBe("0");
    expect(cost).toHaveTextContent("LLM_PRICE_IN_PER_1K, LLM_PRICE_OUT_PER_1K, EFFECT_COST_CLAIM, EFFECT_COST_REISSUE");
    expect(rowCells(cost, /loss \/ high/)[3]).toBe("0,00 €");
    expect(rowCells(cost, /loss \/ high/)[5]).toBe("450,00 €");
  });

  it("Incidencias sin causa: aparecen en una fila «Sin interpretar»", () => {
    setup();
    expect(rowCells(section("Coste por escenario"), /Sin interpretar/)[1]).toBe("1");
  });

  it("Latencia y validaciones de un agente, con ayuda del p95; un agente sin llamadas aparece con 0", () => {
    setup();
    const llm = section("Llamadas al LLM por agente");
    expect(rowCells(llm, /planner/)).toEqual(["planner", "20", "2000", "400", "1050 ms", "1900 ms", "75 %"]);
    expect(rowCells(llm, /interpreter/)).toEqual(["interpreter", "0", "0", "0", "—", "—", "—"]);
    expect(within(llm).getByRole("columnheader", { name: "p95" })).toHaveAttribute("title", expect.stringContaining("rango más próximo"));
  });

  it("Distribución por versión del plan y fallos por acción", () => {
    setup();
    const replan = section("Replanificación y pasos fallidos");
    expect(within(replan).getByRole("list", { name: "Incidencias por versión del plan" })).toHaveTextContent("v1: 40v2: 10");
    expect(rowCells(replan, /reserve_stock/)).toEqual(["reserve_stock", "out_of_stock", "10"]);
    expect(rowCells(replan, /file_claim/)).toEqual(["file_claim", "timeout", "2"]);
  });

  it("Consultas A2A en la versión 2: answered y timeout con sus rondas y latencias medias", () => {
    const data = operationsData();
    data.carriers.consults = [
      { outcome: "answered", consults: 8, mean_rounds: 1, mean_latency_ms: 200 },
      { outcome: "timeout", consults: 2, mean_rounds: 2, mean_latency_ms: 5000 },
    ];
    setup({ data });
    const consults = within(section("Transportistas")).getByRole("table", { name: "Consultas A2A" });
    expect(rowCells(consults, /answered/)).toEqual(["answered", "8", "1", "200 ms"]);
    expect(rowCells(consults, /timeout/)).toEqual(["timeout", "2", "2", "5000 ms"]);
  });

  it("Versión 1: no hay tabla de consultas y carrier_dashboard se ve igual", () => {
    setup();
    const carriers = section("Transportistas");
    expect(within(carriers).queryByRole("table", { name: "Consultas A2A" })).not.toBeInTheDocument();
    expect(rowCells(carriers, /SEUR/)).toEqual(["SEUR", "3", "1", "1", "1"]);
    expect(carriers).toHaveTextContent("POC_VERSION=2");
  });

  it("Proyector al día y proyector atrasado: lag 0 sin destacar y lag 3 destacado en la partición y en el total", () => {
    setup();
    const fresh = section("Frescura de las vistas");
    expect(fresh).toHaveTextContent("consistencia eventual");
    const cells = within(fresh).getAllByRole("cell").filter((c) => c.className === "operations__lag");
    expect(cells.map((c) => c.textContent)).toEqual(["3", "3"]);
    expect(rowCells(fresh, /event_log\s*1/)).toEqual(["event_log", "1", "129", "130", "0"]);
  });

  it("Kafka sin respuesta: las posiciones aplicadas se ven y el lag dice «sin datos»", () => {
    const data = operationsData({
      freshness: { consumers: [{ consumer: "operations", partitions: [{ partition: 0, applied: 12, end_offset: null, lag: null }], lag: null }] },
    });
    setup({ data });
    const fresh = section("Frescura de las vistas");
    expect(rowCells(fresh, /operations\s*0/)).toEqual(["operations", "0", "12", "—", "sin datos"]);
  });

  it("Primer arranque: contadores a 0, desglose vacío y «Sin incidencias todavía»", () => {
    const data = operationsData({ incidents: [] });
    data.summary = { ...data.summary, by_state: { open: 0, resolved: 0, refunded: 0, escalated: 0 }, escalations: [] };
    setup({ data });
    const counters = within(view()).getByRole("list", { name: "Incidencias por estado" });
    expect(within(counters).getAllByRole("button").map((b) => b.querySelector("strong")?.textContent)).toEqual(["0", "0", "0", "0"]);
    expect(screen.getByText("Sin incidencias escaladas")).toBeInTheDocument();
    expect(screen.getByText("Sin incidencias todavía")).toBeInTheDocument();
  });

  it("Fallo de carga: error en rojo encima de las tablas", () => {
    setup({ error: "HTTP 500" });
    expect(screen.getByText("HTTP 500")).toHaveClass("events__error");
  });

  it("Vuelta a pedidos y a la vista principal", async () => {
    const { user, onOrders, onBack } = setup();
    await user.click(screen.getByRole("link", { name: "← Estado de los pedidos" }));
    await user.click(screen.getByRole("link", { name: "← Vista principal" }));
    expect(onOrders).toHaveBeenCalled();
    expect(onBack).toHaveBeenCalled();
  });
});

describe("formatDuration", () => {
  it("formats seconds and minutes, and leaves an open incident empty", () => {
    expect(formatDuration(null)).toBe("");
    expect(formatDuration(12.5)).toBe("12,5 s");
    expect(formatDuration(125)).toBe("2 min 5 s");
  });
});
