import { useCallback, useEffect, useRef, useState } from "react";
import { type ApiClient, ApiError, commandStreamUrl, type SupportRequest } from "./api/client";
import { type OpenStream, type StreamLike, cleanOutput, openEventSource } from "./useCommandRun";

// Las tres acciones que escriben; `status` es el diagnóstico.
export type SupportAction = "republish" | "replay-from" | "rebuild";

// Argumentos de cada tarjeta: los de la petición sin `action` ni `confirm`.
export type SupportArgs =
  | { action: "republish"; mode: "truncated" | "recreated" }
  | { action: "replay-from"; component: string; outbox_id: number }
  | { action: "rebuild"; view: string };

export interface SupportPlan {
  changes: Record<string, unknown>;
  refusal: string | null;
  token: string | null;
}

export interface SupportCard {
  // Simulación de estos argumentos (`argsKey`); «Ejecutar» solo vale para ellos.
  plan?: SupportPlan;
  argsKey?: string;
  // Resultado de la última ejecución confirmada.
  outcome?: string;
  result?: unknown;
  error?: string;
}

export type SupportStatus =
  | { kind: "idle" | "loading" }
  | { kind: "ok"; report: Record<string, unknown> }
  | { kind: "error"; message: string };

interface Finished {
  code: number;
  text: string;
  json: Record<string, unknown> | null;
}

// Clave de unos argumentos: la simulación y la ejecución deben ser de los mismos.
export function argsKey(args: SupportArgs): string {
  return JSON.stringify(args);
}

// `eda-support --json` escribe un único objeto JSON en una línea; stderr (el motivo de una negativa) va aparte.
function lastJson(text: string): Record<string, unknown> | null {
  const lines = text.split("\n").reverse();
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("{")) continue;
    try {
      return JSON.parse(trimmed) as Record<string, unknown>;
    } catch {
      return null;
    }
  }
  return null;
}

function message(error: unknown): string {
  return error instanceof ApiError ? error.message : String(error);
}

// Contenedor: lanza `eda-support` por `POST /api/support/runs`, sigue su salida por SSE y guarda el diagnóstico y la
// simulación y el resultado de cada tarjeta. Una ejecución a la vez (el backend comparte la ejecución única).
export function useSupport(client: ApiClient, active: boolean, openStream: OpenStream = openEventSource) {
  const [status, setStatus] = useState<SupportStatus>({ kind: "idle" });
  const [cards, setCards] = useState<Partial<Record<SupportAction, SupportCard>>>({});
  const [busy, setBusy] = useState(false);
  const stream = useRef<StreamLike | null>(null);
  const opened = useRef(false);

  useEffect(() => () => stream.current?.close(), []);

  const launch = useCallback(
    async (request: SupportRequest): Promise<Finished> => {
      const started = await client.runSupport(request);
      return new Promise<Finished>((resolve) => {
        let text = "";
        const source = openStream(commandStreamUrl(started.run_id));
        stream.current = source;
        source.addEventListener("output", (event) => {
          text += cleanOutput((JSON.parse(event.data) as { text: string }).text);
        });
        source.addEventListener("exit", (event) => {
          source.close();
          resolve({ code: (JSON.parse(event.data) as { code: number }).code, text, json: lastJson(text) });
        });
      });
    },
    [client, openStream],
  );

  const refresh = useCallback(async () => {
    setBusy(true);
    setStatus({ kind: "loading" });
    try {
      const done = await launch({ action: "status" });
      setStatus(done.json ? { kind: "ok", report: done.json } : { kind: "error", message: done.text || `código ${done.code}` });
    } catch (error) {
      setStatus({ kind: "error", message: message(error) });
    } finally {
      setBusy(false);
    }
  }, [launch]);

  // El diagnóstico se pide una vez al abrir la vista; después, solo con «Actualizar».
  useEffect(() => {
    if (active && !opened.current) {
      opened.current = true;
      void refresh();
    }
  }, [active, refresh]);

  const update = (action: SupportAction, card: SupportCard) => setCards((current) => ({ ...current, [action]: card }));

  const simulate = useCallback(
    async (args: SupportArgs) => {
      setBusy(true);
      try {
        const done = await launch(args as SupportRequest);
        const plan = done.json?.plan as SupportPlan | undefined;
        update(args.action, plan ? { plan, argsKey: argsKey(args) } : { error: done.text || `código ${done.code}` });
      } catch (error) {
        update(args.action, { error: message(error) });
      } finally {
        setBusy(false);
      }
    },
    [launch],
  );

  const execute = useCallback(
    async (args: SupportArgs) => {
      const card = cards[args.action];
      const token = card?.plan?.token;
      if (!card || !token || card.argsKey !== argsKey(args)) return;
      setBusy(true);
      try {
        const done = await launch({ ...args, confirm: token } as SupportRequest);
        // La simulación ya no vale: el estado cambió con la ejecución.
        update(args.action, done.json
          ? { outcome: String(done.json.outcome), result: done.json.result }
          : { error: done.text || `código ${done.code}` });
      } catch (error) {
        update(args.action, { ...card, error: message(error) });
      } finally {
        setBusy(false);
      }
    },
    [cards, launch],
  );

  return { status, cards, busy, refresh, simulate, execute };
}
