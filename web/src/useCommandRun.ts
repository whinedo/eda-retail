import { useCallback, useEffect, useRef, useState } from "react";
import { type ApiClient, ApiError, type CommandRequest, commandStreamUrl } from "./api/client";

// Lo que el hook necesita de un EventSource (los tests usan uno falso).
export interface StreamLike {
  addEventListener(type: string, listener: (event: MessageEvent) => void): void;
  close(): void;
}

export type OpenStream = (url: string) => StreamLike;

export const openEventSource: OpenStream = (url) => new EventSource(url);

export type CommandRun =
  | { status: "idle" }
  | { status: "error"; message: string; argv?: string[]; text?: string }
  | {
      status: "running" | "done";
      runId: string;
      argv: string[];
      interactive: boolean;
      text: string;
      exitCode?: number;
      seconds?: number;
    };

// Secuencias ANSI (colores, cursor) que el menú interactivo escribe en la pseudoterminal.
const ANSI = /\u001b\[[0-9;?]*[ -/]*[@-~]/g;

export function cleanOutput(text: string): string {
  return text.replace(ANSI, "").replace(/\r\n?/g, "\n");
}

// Contenedor: lanza un comando, sigue su salida por SSE y envía líneas a `interactive`.
export function useCommandRun(client: ApiClient, openStream: OpenStream = openEventSource) {
  const [run, setRun] = useState<CommandRun>({ status: "idle" });
  const stream = useRef<StreamLike | null>(null);

  useEffect(() => () => stream.current?.close(), []);

  const start = useCallback(
    async (request: CommandRequest) => {
      stream.current?.close();
      let started;
      try {
        started = await client.runCommand(request);
      } catch (error) {
        setRun({ status: "error", message: error instanceof ApiError ? error.message : String(error) });
        return;
      }
      const startedAt = performance.now();
      setRun({
        status: "running",
        runId: started.run_id,
        argv: started.argv,
        interactive: request.command === "interactive",
        text: "",
      });
      const source = openStream(commandStreamUrl(started.run_id));
      stream.current = source;
      source.addEventListener("output", (event) => {
        const { text } = JSON.parse(event.data) as { text: string };
        setRun((current) => (current.status === "running" ? { ...current, text: current.text + cleanOutput(text) } : current));
      });
      source.addEventListener("exit", (event) => {
        const { code } = JSON.parse(event.data) as { code: number };
        source.close();
        const seconds = (performance.now() - startedAt) / 1000;
        setRun((current) => (current.status === "running" ? { ...current, status: "done", exitCode: code, seconds } : current));
      });
    },
    [client, openStream],
  );

  const sendLine = useCallback(
    async (line: string) => {
      if (run.status !== "running" || !run.interactive) return;
      try {
        await client.sendInput(run.runId, line);
      } catch (error) {
        const message = error instanceof ApiError ? error.message : String(error);
        setRun((current) => (current.status === "running" ? { ...current, text: `${current.text}\n[${message}]\n` } : current));
      }
    },
    [client, run],
  );

  // Vuelve al estado inicial (sin ejecuciones). Una ejecución en curso no se toca.
  const reset = useCallback(() => {
    setRun((current) => {
      if (current.status === "running") return current;
      stream.current?.close();
      return { status: "idle" };
    });
  }, []);

  return { run, start, sendLine, reset };
}
