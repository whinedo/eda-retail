import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ApiError, type ApiClient, type CommandRequest } from "../api/client";
import type { StreamLike } from "../useCommandRun";
import { CommandPanel } from "./CommandPanel";

class FakeStream implements StreamLike {
  static last: FakeStream | null = null;
  readonly listeners = new Map<string, (e: MessageEvent) => void>();
  closed = false;

  constructor(readonly url: string) {
    FakeStream.last = this;
  }

  addEventListener(type: string, listener: (e: MessageEvent) => void) {
    this.listeners.set(type, listener);
  }

  close() {
    this.closed = true;
  }

  emit(type: "output" | "exit", data: unknown) {
    act(() => this.listeners.get(type)?.(new MessageEvent(type, { data: JSON.stringify(data) })));
  }
}

const ARGV: Record<string, string[]> = {
  seed: ["scripts/notice.sh", "seed"],
  send: ["scripts/notice.sh", "send"],
};

function setup(runCommand?: ApiClient["runCommand"], freeText = false, interactive = true) {
  const client = {
    runCommand:
      runCommand ??
      vi.fn(async (request: CommandRequest) => {
        const args =
          request.command === "send" && request.example
            ? ["scripts/notice.sh", "send", "--example", request.example]
            : request.command === "send" && request.text
              ? ["scripts/notice.sh", "send", `--text=${request.text}`]
              : request.command === "interactive"
                ? ["scripts/notice.sh", "interactive", "--timeout", String(request.timeout)]
                : ARGV[request.command];
        return { run_id: "run-1", argv: args };
      }),
    sendInput: vi.fn(async () => undefined),
  } as unknown as ApiClient;
  const openStream = vi.fn((url: string) => new FakeStream(url));
  render(<CommandPanel client={client} openStream={openStream} freeText={freeText} interactive={interactive} />);
  return { client, openStream, user: userEvent.setup() };
}

const output = () => screen.getByRole("log", { name: "Salida" });

describe("Menú de comandos", () => {
  it("Ejecutar un aviso de ejemplo", async () => {
    const { client, openStream, user } = setup();

    await user.click(screen.getByRole("button", { name: "loss" }));

    expect(client.runCommand).toHaveBeenCalledWith({ command: "send", example: "loss" });
    expect(openStream).toHaveBeenCalledWith("/api/commands/run-1/stream");
    expect(output()).toHaveTextContent("$ scripts/notice.sh send --example loss");
    FakeStream.last!.emit("output", { text: "aviso N-0007 registrado; incidencia INC-0007\r\n" });
    FakeStream.last!.emit("exit", { code: 0 });
    expect(output()).toHaveTextContent("aviso N-0007 registrado; incidencia INC-0007");
    expect(output()).toHaveTextContent(/terminó con código 0 en \d+,\d s/);
    expect(FakeStream.last!.closed).toBe(true);
  });

  it("Preparar los datos de ejemplo: seed va solo en Preparación y avisa de que no genera eventos", () => {
    setup();
    const prep = screen.getByRole("region", { name: "Preparación" });
    const notices = screen.getByRole("region", { name: "Avisos" });

    expect(within(prep).getAllByRole("button").map((b) => b.textContent)).toEqual([
      expect.stringMatching(/^seed/),
    ]);
    expect(prep).toHaveTextContent("No genera eventos");
    expect(within(notices).queryByRole("button", { name: /^seed/ })).not.toBeInTheDocument();
    expect(within(notices).getByRole("button", { name: /^send\b/ })).toBeInTheDocument();
    expect(within(notices).getByRole("button", { name: "loss" })).toBeInTheDocument();
    expect(within(notices).getByRole("button", { name: /^interactive/ })).toBeInTheDocument();
  });

  it("ofrece los cuatro ejemplos de scripts/notice.sh", () => {
    setup();
    for (const example of ["delay", "loss", "out_of_stock", "injection"]) {
      expect(screen.getByRole("button", { name: example })).toBeInTheDocument();
    }
  });

  it("Descripción de los ejemplos: cada ejemplo dice lo que provoca", () => {
    setup();
    const descriptions = screen.getByRole("list", { name: "Qué provoca cada ejemplo" });
    const items = within(descriptions).getAllByRole("listitem").map((item) => item.textContent);

    expect(items).toEqual([
      "delay: Retraso de 24 h en P-1001: el plan avisa al cliente.",
      "loss: Pérdida de P-1002: reclamar, reservar stock y reenviar.",
      "out_of_stock: Pérdida de P-1004, sin stock: la reserva falla y se revisa el plan.",
      "injection: Retraso de P-1003 con una orden inyectada (reembolsar 1000 EUR): se trata como dato y los reembolsos tienen un tope.",
    ]);
    expect(screen.getByRole("button", { name: "out_of_stock" })).toHaveAttribute(
      "title",
      "Pérdida de P-1004, sin stock: la reserva falla y se revisa el plan.",
    );
  });

  it("seed y send sin argumentos", async () => {
    const { client, user } = setup();

    await user.click(screen.getByRole("button", { name: /^seed/ }));
    FakeStream.last!.emit("exit", { code: 0 });
    await user.click(screen.getByRole("button", { name: /^send\b/ }));

    expect(client.runCommand).toHaveBeenNthCalledWith(1, { command: "seed" });
    expect(client.runCommand).toHaveBeenNthCalledWith(2, { command: "send" });
  });

  it("Interactive deshabilitado: el menú no muestra interactive ni su timeout", () => {
    setup(undefined, false, false);

    expect(screen.queryByRole("button", { name: /^interactive/ })).not.toBeInTheDocument();
    expect(screen.queryByRole("spinbutton", { name: "timeout" })).not.toBeInTheDocument();
  });

  it("Texto libre deshabilitado: el menú no muestra send --text", () => {
    setup();

    expect(screen.queryByRole("textbox", { name: "Texto libre del aviso" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Enviar aviso" })).not.toBeInTheDocument();
    expect(screen.queryByText("send --text")).not.toBeInTheDocument();
  });

  it("Texto libre: se envía el texto escrito y el botón se desactiva sin texto", async () => {
    const { client, user } = setup(undefined, true);
    const send = screen.getByRole("button", { name: "Enviar aviso" });
    expect(send).toBeDisabled();

    await user.type(screen.getByRole("textbox", { name: "Texto libre del aviso" }), "El pedido P-1003 llega tarde");
    await user.click(send);

    expect(client.runCommand).toHaveBeenCalledWith({ command: "send", text: "El pedido P-1003 llega tarde" });
  });

  it("Agentes parados: el aviso de notice.sh aparece tal cual", async () => {
    const { user } = setup();
    const warning = "aviso: los agentes no están en marcha (scripts/run-agents.sh); los avisos no avanzarán";

    await user.click(screen.getByRole("button", { name: /^send\b/ }));
    FakeStream.last!.emit("output", { text: `${warning}\n` });

    expect(output()).toHaveTextContent(warning);
  });

  it("Segunda ejecución simultánea: los comandos se desactivan mientras uno se ejecuta", async () => {
    const { user } = setup();

    await user.click(screen.getByRole("button", { name: "delay" }));

    expect(screen.getByRole("button", { name: "loss" })).toBeDisabled();
    expect(screen.getByRole("button", { name: /^seed/ })).toBeDisabled();
    FakeStream.last!.emit("exit", { code: 0 });
    expect(screen.getByRole("button", { name: "loss" })).toBeEnabled();
  });

  it("muestra el rechazo del backend (409) en la salida", async () => {
    const { user } = setup(vi.fn(async () => Promise.reject(new ApiError(409, "ya hay un comando en ejecución"))));

    await user.click(screen.getByRole("button", { name: /^seed/ }));

    expect(output()).toHaveTextContent("ya hay un comando en ejecución");
    expect(screen.getByRole("button", { name: /^seed/ })).toBeEnabled();
  });

  it("quita las secuencias de color ANSI de la salida", async () => {
    const { user } = setup();

    await user.click(screen.getByRole("button", { name: /^seed/ }));
    FakeStream.last!.emit("output", { text: "\u001b[32mlisto\u001b[0m\r\n" });

    expect(output().textContent).toContain("listo\n");
    expect(output().textContent).not.toContain("\u001b");
  });
});

describe("Comando interactivo", () => {
  it("Responder al menú", async () => {
    const { client, user } = setup();
    expect(screen.queryByRole("textbox", { name: "Entrada" })).not.toBeInTheDocument();

    const timeout = screen.getByRole("spinbutton", { name: "timeout" });
    await user.clear(timeout);
    await user.type(timeout, "60");
    await user.click(screen.getByRole("button", { name: /^interactive/ }));

    expect(client.runCommand).toHaveBeenCalledWith({ command: "interactive", timeout: 60 });
    FakeStream.last!.emit("output", { text: "Elige un aviso:\r\n" });
    await user.type(screen.getByRole("textbox", { name: "Entrada" }), "2{Enter}");

    expect(client.sendInput).toHaveBeenCalledWith("run-1", "2");
    expect(screen.getByRole("textbox", { name: "Entrada" })).toHaveValue("");
    FakeStream.last!.emit("exit", { code: 0 });
    expect(screen.queryByRole("textbox", { name: "Entrada" })).not.toBeInTheDocument();
  });

  it("el timeout debe estar entre 1 y 600", async () => {
    const { user } = setup();
    const timeout = screen.getByRole("spinbutton", { name: "timeout" });

    await user.clear(timeout);
    await user.type(timeout, "601");

    expect(screen.getByRole("button", { name: /^interactive/ })).toBeDisabled();
  });
});
