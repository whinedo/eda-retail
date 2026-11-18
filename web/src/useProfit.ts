import { useEffect, useState } from "react";
import type { ApiClient, Profit } from "./api/client";
import { OPERATIONS_INTERVAL_MS } from "./useOperations";

export interface ProfitState {
  data: Profit | null;
  error: string | null;
}

// Contenedor de la pantalla de beneficio (spec operations-view): consulta /api/operations/profit mientras está abierta,
// con el mismo intervalo que la vista de operaciones; un fallo se muestra y desaparece con la siguiente respuesta.
export function useProfit(client: ApiClient, enabled: boolean, intervalMs = OPERATIONS_INTERVAL_MS): ProfitState {
  const [data, setData] = useState<Profit | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!enabled) return;
    let active = true;
    const refresh = () =>
      client.getOperationsProfit().then(
        (profit) => {
          if (!active) return;
          setData(profit);
          setError(null);
        },
        (err: unknown) => active && setError(err instanceof Error ? err.message : String(err)),
      );
    refresh();
    const timer = setInterval(refresh, intervalMs);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [client, enabled, intervalMs]);

  return { data, error };
}
