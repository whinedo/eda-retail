import { useEffect, useState } from "react";
import type { ApiClient, Health } from "./api/client";

export type HealthState = { kind: "loading" } | { kind: "ok"; health: Health } | { kind: "offline" };

export const HEALTH_INTERVAL_MS = 5000;

// Contenedor: consulta /api/health al montar y cada HEALTH_INTERVAL_MS.
export function useHealth(client: ApiClient, intervalMs = HEALTH_INTERVAL_MS): HealthState {
  const [state, setState] = useState<HealthState>({ kind: "loading" });

  useEffect(() => {
    let active = true;
    const refresh = () =>
      client.getHealth().then(
        (health) => active && setState({ kind: "ok", health }),
        () => active && setState({ kind: "offline" }),
      );
    refresh();
    const timer = setInterval(refresh, intervalMs);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [client, intervalMs]);

  return state;
}
