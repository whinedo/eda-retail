import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { EventRow } from "../api/client";
import { EventsPanel, type EventsPanelProps } from "./EventsPanel";

function row(seq: number, overrides: Partial<EventRow> = {}): EventRow {
  return {
    seq,
    event_id: "d282ef26-1a2b-4c3d-9e8f-001122334455",
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
    payload: { incident_id: "INC-0007" },
    ...overrides,
  };
}

function setup(props: Partial<EventsPanelProps> = {}) {
  const handlers = {
    onIncidentChange: vi.fn(),
    onClearNode: vi.fn(),
    onTogglePause: vi.fn(),
    onLoadMore: vi.fn(),
  };
  render(
    <EventsPanel rows={[row(1303)]} paused={false} incidentId="" node={null} hasMore={false} error={null} {...handlers} {...props} />,
  );
  return { ...handlers, user: userEvent.setup() };
}

const COLUMNS = [
  "Emisor",
  "Destino",
  "Tipo de evento",
  "Timestamp",
  "seq",
  "incident_id",
  "plan",
  "partición / offset",
  "event_id",
  "causation_id",
  "payload",
];

const cells = (seq: number) =>
  within(screen.getByRole("row", { name: new RegExp(`\\b${seq}\\b`) }))
    .getAllByRole("cell")
    .map((c) => c.textContent);

describe("Panel de eventos intercambiados", () => {
  it("Desplazamiento lateral: todas las columnas en orden, tabla con scroll y cabecera fija", () => {
    setup();

    const region = screen.getByRole("region", { name: "Eventos intercambiados" });
    const table = within(region).getByRole("table");
    expect(within(table).getAllByRole("columnheader").map((h) => h.textContent)).toEqual(COLUMNS);
    expect(table.parentElement).toHaveClass("events__scroll");
    expect(table.querySelector("thead")).toHaveClass("events__head");
    expect(screen.getByText("Desplaza hacia la derecha para ver más columnas")).toBeInTheDocument();
  });

  it("Evento con consumidor único: emisor y destino salen de sender y receiver", () => {
    setup();

    const [sender, receiver, type] = cells(1303);
    expect([sender, receiver, type]).toEqual(["planner", "orchestrator", "PlanCreated"]);
  });

  it("Evento sin partición registrada: la columna partición / offset muestra —", () => {
    setup({ rows: [row(1303), row(1302, { partition: 1, offset: 52, plan_version: null })] });

    expect(cells(1303)[7]).toBe("—");
    expect(cells(1302)[7]).toBe("p1 / 52");
    expect(cells(1303)[6]).toBe("v1");
    expect(cells(1302)[6]).toBe("—");
  });

  it("acorta los ids largos con puntos suspensivos y deja el valor completo en el title", () => {
    setup();

    const id = screen.getByTitle("d282ef26-1a2b-4c3d-9e8f-001122334455");
    expect(id).toHaveTextContent("d282ef26…");
  });

  it("muestra StepFailed en el color de caído y el destino con todos sus consumidores en el title", () => {
    setup({
      rows: [
        row(1311, {
          event_type: "StepFailed",
          sender: "executor-reissue_shipment",
          receiver: "orchestrator",
          receivers: ["orchestrator", "replanner", "projectors"],
        }),
      ],
    });

    expect(screen.getByText("StepFailed")).toHaveClass("event-type--failed");
    expect(screen.getByTitle("orchestrator, replanner, projectors")).toHaveTextContent("orchestrator");
  });

  it("indica en vivo o en pausa, cuenta las filas y alterna Pausar / Reanudar", async () => {
    const { user, onTogglePause } = setup({ rows: [row(2), row(1)] });

    expect(screen.getByText("en vivo")).toBeInTheDocument();
    expect(screen.getByText("2 eventos")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Pausar" }));
    expect(onTogglePause).toHaveBeenCalledTimes(1);
  });

  it("en pausa ofrece Reanudar", () => {
    setup({ paused: true });

    expect(screen.getByText("en pausa")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Reanudar" })).toBeInTheDocument();
  });

  it("aplica el filtro de incidencia al pulsar Intro", async () => {
    const { user, onIncidentChange } = setup();

    await user.type(screen.getByLabelText("Incidencia"), "INC-0007{Enter}");

    expect(onIncidentChange).toHaveBeenCalledWith("INC-0007");
  });

  it("muestra el filtro de nodo como chip que se puede quitar", async () => {
    const { user, onClearNode } = setup({ node: "planner" });

    await user.click(screen.getByRole("button", { name: "Quitar filtro nodo: planner" }));

    expect(onClearNode).toHaveBeenCalledTimes(1);
  });

  it("ofrece Cargar más solo si hay página anterior", async () => {
    const { user, onLoadMore } = setup({ hasMore: true });

    await user.click(screen.getByRole("button", { name: "Cargar más" }));

    expect(onLoadMore).toHaveBeenCalledTimes(1);
  });

  it("no ofrece Cargar más sin página anterior", () => {
    setup({ hasMore: false });

    expect(screen.queryByRole("button", { name: "Cargar más" })).not.toBeInTheDocument();
  });
});
