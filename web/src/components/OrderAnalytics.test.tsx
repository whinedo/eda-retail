import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { OrderAnalytics as Data, OrderStatusRow } from "../api/client";
import { OrderAnalytics, type OrderAnalyticsProps } from "./OrderAnalytics";

function order(order_id: string, overrides: Partial<OrderStatusRow> = {}): OrderStatusRow {
  return {
    order_id,
    incident_id: "INC-0007",
    status: "delayed",
    last_event_type: "ShipmentDelayed",
    last_event_id: "d282ef26-1a2b-4c3d-9e8f-001122334455",
    occurred_at: "2026-11-14T01:48:19.234567+00:00",
    kafka_partition: 2,
    kafka_offset: 1290,
    ...overrides,
  };
}

const DATA: Data = {
  counts: { delayed: 1, in_resolution: 0, resolved: 0, refunded: 1, escalated: 0 },
  orders: [
    order("P-1001", { status: "refunded", last_event_type: "IncidentResolved" }),
    order("P-1002", { incident_id: "INC-0008" }),
  ],
  applied: { "2": 1290 },
};

function setup(props: Partial<OrderAnalyticsProps> = {}) {
  const handlers = {
    onStatusChange: vi.fn(),
    onIncidentChange: vi.fn(),
    onTogglePause: vi.fn(),
    onBack: vi.fn(),
  };
  render(<OrderAnalytics data={DATA} error={null} status={null} incidentId="" paused={false} {...handlers} {...props} />);
  return { ...handlers, user: userEvent.setup() };
}

const cells = (orderId: string) =>
  within(screen.getByRole("row", { name: new RegExp(orderId) }))
    .getAllByRole("cell")
    .map((c) => c.textContent);

describe("Vista de analítica de pedidos", () => {
  it("Desplazamiento lateral en la analítica: tabla con scroll y cabecera fija", () => {
    setup();

    const table = within(screen.getByRole("region", { name: "Estado de los pedidos" })).getByRole("table");
    expect(within(table).getAllByRole("columnheader").map((h) => h.textContent)).toEqual([
      "Pedido",
      "Estado",
      "Incidencia",
      "Último evento",
      "Timestamp",
      "partición / offset",
      "event_id",
    ]);
    expect(table.parentElement).toHaveClass("events__scroll");
    expect(table.querySelector("thead")).toHaveClass("events__head");
    expect(screen.getByText("Desplaza hacia la derecha para ver más columnas")).toBeInTheDocument();
  });

  it("muestra cada pedido con su estado, su incidencia y el último evento", () => {
    setup();

    expect(cells("P-1001")).toEqual([
      "P-1001",
      "Reembolsada",
      "INC-0007",
      "IncidentResolved",
      "2026-11-14 01:48:19.234",
      "p2 / 1290",
      "d282ef26…",
    ]);
    expect(cells("P-1002")[1]).toBe("Envío retrasado");
  });

  it("un contador por estado con su etiqueta, también los que están a cero", () => {
    setup();

    const counters = within(screen.getByRole("list", { name: "Pedidos por estado" })).getAllByRole("button");
    expect(counters.map((c) => c.textContent)).toEqual([
      "1Envío retrasado",
      "0En proceso de solución",
      "0Solucionada",
      "1Reembolsada",
      "0Escalada",
    ]);
  });

  it("Filtro por estado: pulsar un contador filtra y pulsarlo otra vez quita el filtro", async () => {
    const { user, onStatusChange } = setup();
    await user.click(screen.getByRole("button", { name: /Envío retrasado/ }));
    expect(onStatusChange).toHaveBeenLastCalledWith("delayed");

    onStatusChange.mockClear();
    const filtered = setup({ status: "delayed" });
    const pressed = screen.getAllByRole("button", { name: /Envío retrasado/, pressed: true });
    await filtered.user.click(pressed[0]);
    expect(filtered.onStatusChange).toHaveBeenLastCalledWith(null);
  });

  it("filtra por incidencia al pulsar Intro, como el panel de eventos", async () => {
    const { user, onIncidentChange } = setup();

    await user.type(screen.getByLabelText("Incidencia"), "INC-0007{Enter}");

    expect(onIncidentChange).toHaveBeenCalledWith("INC-0007");
  });

  it("Pausar y Reanudar", async () => {
    const { user, onTogglePause } = setup();
    expect(screen.getByText("en vivo")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Pausar" }));
    expect(onTogglePause).toHaveBeenCalled();
  });

  it("vuelve a la vista principal con el enlace de vuelta", async () => {
    const { user, onBack } = setup();
    await user.click(screen.getByRole("link", { name: "← Vista principal" }));
    expect(onBack).toHaveBeenCalled();
  });
});
