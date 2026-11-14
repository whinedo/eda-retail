import { useEffect, useState } from "react";
import type { ApiClient } from "./api/client";

// Valor del backend por defecto (JAEGER_UI_URL); se usa mientras carga o si /api/config falla.
export const DEFAULT_JAEGER_UI_URL = "http://localhost:16686";

// Contenedor: lee /api/config una vez al montar.
export function useJaegerUrl(client: ApiClient): string {
  const [url, setUrl] = useState(DEFAULT_JAEGER_UI_URL);

  useEffect(() => {
    let active = true;
    client.getConfig().then(
      (config) => active && setUrl(config.jaeger_ui_url),
      () => undefined,
    );
    return () => {
      active = false;
    };
  }, [client]);

  return url;
}
