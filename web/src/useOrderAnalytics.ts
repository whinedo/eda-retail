import { useCallback, useEffect, useState } from "react";
import type { ApiClient, OrderAnalytics, OrderStatus } from "./api/client";

// Vista de analítica de pedidos (spec web-front): un cambio aparece en menos de 2 segundos.
export const ANALYTICS_INTERVAL_MS = 1000;

export interface OrderAnalyticsState {
  data: OrderAnalytics | null;
  error: string | null;
  status: OrderStatus | null;
  incidentId: string;
  paused: boolean;
  setStatus(status: OrderStatus | null): void;
  setIncidentId(incidentId: string): void;
  pause(): void;
  resume(): void;
}

// Contenedor: consulta /api/analytics/orders mientras la vista está abierta y no en pausa.
// Al cambiar un filtro consulta enseguida, también en pausa.
export function useOrderAnalytics(
  client: ApiClient,
  enabled: boolean,
  intervalMs = ANALYTICS_INTERVAL_MS,
): OrderAnalyticsState {
  const [data, setData] = useState<OrderAnalytics | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<OrderStatus | null>(null);
  const [incidentId, setIncidentId] = useState("");
  const [paused, setPaused] = useState(false);

  useEffect(() => {
    if (!enabled) return;
    let active = true;
    const refresh = () =>
      client.getOrderAnalytics({ status: status ?? undefined, incidentId: incidentId || undefined }).then(
        (next) => {
          if (!active) return;
          setData(next);
          setError(null);
        },
        (err: unknown) => active && setError(err instanceof Error ? err.message : String(err)),
      );
    refresh();
    const timer = paused ? undefined : setInterval(refresh, intervalMs);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [client, enabled, status, incidentId, paused, intervalMs]);

  const pause = useCallback(() => setPaused(true), []);
  const resume = useCallback(() => setPaused(false), []);
  return { data, error, status, incidentId, paused, setStatus, setIncidentId, pause, resume };
}
