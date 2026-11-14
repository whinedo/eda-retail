import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import App from "./App";
import type { ApiClient, Health } from "./api/client";
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
    getTopology: vi.fn(async () => ({ nodes: [], edges: [] })),
    getEvents: vi.fn(async () => ({ events: [], next_before_seq: null })),
    getNode: vi.fn(),
    runCommand: vi.fn(),
    sendInput: vi.fn(),
    ...overrides,
  } as ApiClient;
}

class FakeStream implements StreamLike {
  static all: FakeStream[] = [];
  closed = false;
  constructor(readonly url: string) {
    FakeStream.all.push(this);
  }
  addEventListener() {}
  close() {
    this.closed = true;
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
});
