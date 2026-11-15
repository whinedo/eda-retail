import { useEffect, useState } from "react";
import type { ApiClient, ParkedNotice } from "./api/client";

// Aviso de avisos aparcados (spec web-front): un cambio aparece en menos de 2 segundos.
export const PARKED_NOTICES_INTERVAL_MS = 1000;

// Contenedor: consulta /api/parked-notices al montar y cada PARKED_NOTICES_INTERVAL_MS, en las dos vistas.
// Si una consulta falla conserva los últimos avisos: la cabecera ya muestra si el backend no responde.
export function useParkedNotices(client: ApiClient, intervalMs = PARKED_NOTICES_INTERVAL_MS): ParkedNotice[] {
  const [notices, setNotices] = useState<ParkedNotice[]>([]);

  useEffect(() => {
    let active = true;
    const refresh = () =>
      client.getParkedNotices().then(
        (next) => active && setNotices(next.notices),
        () => undefined,
      );
    refresh();
    const timer = setInterval(refresh, intervalMs);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [client, intervalMs]);

  return notices;
}
