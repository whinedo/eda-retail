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
  // Versión de la PoC del backend (POC_VERSION); null mientras carga o si /api/config no la trae.
  pocVersion: PocVersion | null;
  // Soporte técnico solo si el backend lo admite (WEB_SUPPORT); mientras carga, deshabilitado.
  support: boolean;
  // Argumentos que admite la lista blanca de soporte: componentes con outbox y vistas.
  supportComponents: string[];
  supportViews: string[];
}

export type PocVersion = 1 | 2;

function pocVersion(raw: unknown): PocVersion | null {
  return raw === 1 || raw === 2 ? raw : null;
}

function strings(raw: unknown): string[] {
  return Array.isArray(raw) ? raw.filter((item): item is string => typeof item === "string") : [];
}

// Contenedor: lee /api/config una vez al montar.
export function useConfig(client: ApiClient): Config {
  const [config, setConfig] = useState<Config>({
    jaegerUrl: DEFAULT_JAEGER_UI_URL,
    freeText: false,
    interactive: false,
    pocVersion: null,
    support: false,
    supportComponents: [],
    supportViews: [],
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
          pocVersion: pocVersion(c.poc_version),
          support: c.support_enabled === true,
          supportComponents: strings(c.support_components),
          supportViews: strings(c.support_views),
        }),
      () => undefined,
    );
    return () => {
      active = false;
    };
  }, [client]);

  return config;
}
