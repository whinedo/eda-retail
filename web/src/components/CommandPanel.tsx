import type { ApiClient } from "../api/client";
import { type OpenStream, openEventSource, useCommandRun } from "../useCommandRun";
import { CommandMenu } from "./CommandMenu";

// Contenedor del menú: conecta el estado de la ejecución con el menú presentacional.
export function CommandPanel({ client, openStream = openEventSource }: { client: ApiClient; openStream?: OpenStream }) {
  const { run, start, sendLine } = useCommandRun(client, openStream);
  return <CommandMenu run={run} onRun={(request) => void start(request)} onSendLine={(line) => void sendLine(line)} />;
}
