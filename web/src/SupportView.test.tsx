import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { ApiClient, FrontConfig, SupportRequest } from "./api/client";
import type { StreamLike } from "./useCommandRun";

const STATUS = {
  processes: { orchestrator: false, projectors: false },
  topic: { "0": { low: 0, high: 10 } },
  groups: { interpreter: { members: 0, lag: 0 } },
  read_side: { event_log: 0 },
  outbox: { planner: { rows: 4, pending: 0, max_id: 4 } },
  dead_letters: { planner: { rows: 0, unpublished: 0 } },
  failed_jobs: { planner: 0 },
  parked_notices: 0,
  alerts: ["topic_lost"],
  errors: {},
};

class FakeStream implements StreamLike {
  static all: FakeStream[] = [];
  readonly listeners = new Map<string, (event: MessageEvent) => void>();
  constructor(readonly url: string) {
    FakeStream.all.push(this);
  }
  addEventListener(type: string, listener: (event: MessageEvent) => void) {
    this.listeners.set(type, listener);
  }
  close() {}
  emit(type: string, data: unknown) {
    act(() => this.listeners.get(type)?.(new MessageEvent(type, { data: JSON.stringify(data) })));
  }
  // Lo que imprime `eda-support --json` y el código de salida.
  finish(json: unknown, code = 0, stderr = "") {
    this.emit("output", { text: `${JSON.stringify(json)}\n${stderr}` });
    this.emit("exit", { code });
  }
}

const openStream = (url: string) => new FakeStream(url);

function config(support: boolean): FrontConfig {
  return {
    jaeger_ui_url: "http://jaeger.local:16686",
    free_text_enabled: false,
    interactive_enabled: false,
    support_enabled: support,
    support_components: ["intake", "planner"],
    support_views: ["orders", "operations"],
  };
}

function client(support: boolean) {
  let n = 0;
  const runSupport = vi.fn(async (_request: SupportRequest) => ({ run_id: `r${++n}`, argv: [] }));
  const api = {
    getHealth: vi.fn(async () => Promise.reject(new Error("sin salud"))),
    getConfig: vi.fn(async () => config(support)),
    getTopology: vi.fn(async () => ({ nodes: [], edges: [] })),
    getEvents: vi.fn(async () => ({ events: [], next_before_seq: null })),
    getNode: vi.fn(),
    getOrderAnalytics: vi.fn(),
    getParkedNotices: vi.fn(async () => ({ notices: [], applied: {} })),
    runCommand: vi.fn(),
    runSupport,
    sendInput: vi.fn(),
  } as unknown as ApiClient;
  return { api, runSupport };
}

// La última ejecución lanzada, una vez abierto su stream.
async function lastStream(count: number): Promise<FakeStream> {
  await waitFor(() => expect(FakeStream.all.filter((s) => s.url.startsWith("/api/commands/")).length).toBe(count));
  return FakeStream.all.filter((s) => s.url.startsWith("/api/commands/"))[count - 1];
}

async function openSupport(support = true) {
  window.location.hash = "#/support";
  const { api, runSupport } = client(support);
  render(<App client={api} openStream={openStream} />);
  return { runSupport };
}

beforeEach(() => {
  FakeStream.all = [];
  window.location.hash = "#/";
});

describe("vista de soporte técnico", () => {
  it("Enlace desde la vista principal: «Soporte» junto a «Analítica» abre #/support", async () => {
    const { api } = client(true);
    render(<App client={api} openStream={openStream} />);
    const link = await screen.findByRole("link", { name: "Soporte" });
    const header = link.closest("header") as HTMLElement;
    const names = within(header).getAllByRole("link").map((a) => a.textContent);
    expect(names.indexOf("Soporte")).toBe(names.indexOf("Analítica") + 1);
    await userEvent.click(link);
    expect(window.location.hash).toBe("#/support");
    expect(await screen.findByRole("heading", { name: "Soporte técnico" })).toBeInTheDocument();
  });

  it("Sin enlace con el soporte deshabilitado", async () => {
    const { runSupport } = await openSupport(false);
    expect(await screen.findByText(/El soporte técnico está deshabilitado/)).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Soporte" })).not.toBeInTheDocument();
    expect(runSupport).not.toHaveBeenCalled();
  });

  it("Diagnóstico al abrir la vista: pide status una vez y muestra alertas y secciones", async () => {
    const { runSupport } = await openSupport();
    (await lastStream(1)).finish(STATUS);
    expect(runSupport).toHaveBeenCalledExactlyOnceWith({ action: "status" });
    expect(within(await screen.findByRole("list", { name: "Alertas" })).getByText("topic_lost")).toBeInTheDocument();
    for (const name of ["Procesos", "Grupos de Kafka", "Outbox por componente", "Dead letters", "Trabajos fallidos",
      "Avisos aparcados", "Lag del lado de lectura", "Topic (marcas de agua por partición)"]) {
      expect(screen.getByRole("region", { name })).toBeInTheDocument();
    }
    expect(runSupport).toHaveBeenCalledTimes(1);
    await userEvent.click(screen.getByRole("button", { name: "Actualizar" }));
    expect(runSupport).toHaveBeenCalledTimes(2);
  });

  it("Simular y ejecutar una acción: la ejecución envía el token de su simulación y muestra el resultado", async () => {
    const { runSupport } = await openSupport();
    (await lastStream(1)).finish(STATUS);
    const card = await screen.findByRole("region", { name: "Republicar desde una fila" });
    await userEvent.selectOptions(within(card).getByRole("combobox"), "planner");
    await userEvent.type(within(card).getByRole("spinbutton"), "3");
    const execute = within(card).getByRole("button", { name: "Ejecutar" });
    expect(execute).toBeDisabled();
    await userEvent.click(within(card).getByRole("button", { name: "Simular" }));
    expect(runSupport).toHaveBeenLastCalledWith({ action: "replay-from", component: "planner", outbox_id: 3 });
    (await lastStream(2)).finish({ plan: { changes: { rows: 2 }, refusal: null, token: "0123456789abcdef" } });
    await waitFor(() => expect(execute).toBeEnabled());
    expect(within(card).getByText(/Token 0123456789abcdef/)).toBeInTheDocument();
    await userEvent.click(execute);
    expect(runSupport).toHaveBeenLastCalledWith({ action: "replay-from", component: "planner", outbox_id: 3,
      confirm: "0123456789abcdef" });
    (await lastStream(3)).finish({ outcome: "succeeded", result: { rows: 2 } });
    expect(await within(card).findByText("Resultado: succeeded")).toBeInTheDocument();
  });

  it("Acción negada: muestra el motivo y «Ejecutar» sigue deshabilitado", async () => {
    await openSupport();
    (await lastStream(1)).finish(STATUS);
    const card = await screen.findByRole("region", { name: "Republicar tras perder el topic" });
    await userEvent.click(within(card).getByRole("button", { name: "Simular" }));
    const refusal = "el sistema no está parado: procesos vivos: orchestrator";
    (await lastStream(2)).finish({ plan: { changes: {}, refusal, token: null } }, 2, `No se puede: ${refusal}\n`);
    expect(await within(card).findByText(`No se puede: ${refusal}`)).toBeInTheDocument();
    expect(within(card).getByRole("button", { name: "Ejecutar" })).toBeDisabled();
  });

  it("Argumentos cambiados tras simular: «Ejecutar» se deshabilita hasta una nueva simulación", async () => {
    await openSupport();
    (await lastStream(1)).finish(STATUS);
    const card = await screen.findByRole("region", { name: "Reconstruir una vista" });
    await userEvent.click(within(card).getByRole("button", { name: "Simular" }));
    (await lastStream(2)).finish({ plan: { changes: { tables: {} }, refusal: null, token: "0123456789abcdef" } });
    const execute = within(card).getByRole("button", { name: "Ejecutar" });
    await waitFor(() => expect(execute).toBeEnabled());
    await userEvent.selectOptions(within(card).getByRole("combobox"), "operations");
    expect(execute).toBeDisabled();
  });

  it("Con una ejecución en curso, los botones están deshabilitados", async () => {
    await openSupport();
    await lastStream(1);  // el diagnóstico sigue en curso
    const card = await screen.findByRole("region", { name: "Reconstruir una vista" });
    expect(within(card).getByRole("button", { name: "Simular" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Actualizar" })).toBeDisabled();
  });
});
