import { useEffect, useState } from "react";
import type { ApiClient } from "./api/client";

// Valor del backend por defecto (JAEGER_UI_URL); se usa mientras carga o si /api/config falla.
export const DEFAULT_JAEGER_UI_URL = "http://localhost:16686";

export interface Config {
  jaegerUrl: string;
  // `send --text` solo si el backend lo admite (WEB_FREE_TEXT); mientras carga, deshabilitado.
  freeText: boolean;
  // `interactive` solo si el backend lo admite (WEB_INTERACTIVE); mientras carga, deshabilitado.
  interactive: boolean;
}

// Contenedor: lee /api/config una vez al montar.
export function useConfig(client: ApiClient): Config {
  const [config, setConfig] = useState<Config>({
    jaegerUrl: DEFAULT_JAEGER_UI_URL,
    freeText: false,
    interactive: false,
  });

  useEffect(() => {
    let active = true;
    client.getConfig().then(
      (c) =>
        active &&
        setConfig({
          jaegerUrl: c.jaeger_ui_url,
          freeText: c.free_text_enabled === true,
          interactive: c.interactive_enabled === true,
        }),
      () => undefined,
    );
    return () => {
      active = false;
    };
  }, [client]);

  return config;
}
