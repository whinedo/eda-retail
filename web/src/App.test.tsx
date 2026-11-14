import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import App from "./App";
import type { ApiClient, Health } from "./api/client";

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

describe("Distribución de la pantalla", () => {
  it("Vista en reposo: menú, mapa, panel de eventos y ninguna ejecución", async () => {
    render(<App client={client()} />);

    expect(screen.getByRole("banner")).toHaveTextContent("EDA Retail");
    expect(screen.getByRole("complementary", { name: "Comandos" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Mapa de sistemas" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Eventos intercambiados" })).toBeInTheDocument();
    expect(screen.getByRole("log", { name: "Salida" })).toHaveTextContent("Sin ejecuciones todavía");
  });

  it("Salud de la infraestructura: la cabecera muestra cada servicio y los procesos", async () => {
    render(<App client={client()} />);

    expect(await screen.findByText("PostgreSQL: down")).toBeInTheDocument();
    expect(screen.getByText("Kafka: healthy")).toBeInTheDocument();
    expect(screen.getByText("Ollama: healthy")).toBeInTheDocument();
    expect(screen.getByText("Procesos: 2 de 12")).toBeInTheDocument();
  });

  it("muestra que el backend no responde si falla la consulta de salud", async () => {
    render(<App client={client({ getHealth: vi.fn(async () => Promise.reject(new Error("boom"))) })} />);

    expect(await screen.findByText("Backend: sin conexión")).toBeInTheDocument();
  });

  it("al pulsar un nodo del mapa queda seleccionado", async () => {
    const topology = {
      nodes: [{ id: "relay", label: "Relay", kind: "code" as const, description: "", monitored: true }],
      edges: [],
    };
    render(<App client={client({ getTopology: vi.fn(async () => topology) })} />);

    const relay = await screen.findByRole("button", { name: "Relay" });
    await userEvent.setup().click(relay);

    expect(relay).toHaveAttribute("aria-pressed", "true");
  });
});
