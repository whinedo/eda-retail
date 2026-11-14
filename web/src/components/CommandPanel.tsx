import { useEffect } from "react";
import type { ApiClient } from "../api/client";
import { type OpenStream, openEventSource, useCommandRun } from "../useCommandRun";
import { CommandMenu } from "./CommandMenu";

// Contenedor del menú: conecta el estado de la ejecución con el menú presentacional.
// Cada cambio de `resetKey` vuelve el menú a su estado inicial; `onRunningChange` avisa de si hay
// un comando en ejecución (la cabecera desactiva el reinicio mientras tanto).
export function CommandPanel({
  client,
  openStream = openEventSource,
  freeText = false,
  resetKey = 0,
  onRunningChange,
}: {
  client: ApiClient;
  openStream?: OpenStream;
  freeText?: boolean;
  resetKey?: number;
  onRunningChange?: (running: boolean) => void;
}) {
  const { run, start, sendLine, reset } = useCommandRun(client, openStream);
  const running = run.status === "running";

  useEffect(() => {
    if (resetKey > 0) reset();
  }, [resetKey, reset]);

  useEffect(() => {
    onRunningChange?.(running);
  }, [running, onRunningChange]);

  return <CommandMenu run={run} freeText={freeText} onRun={(request) => void start(request)} onSendLine={(line) => void sendLine(line)} />;
}
