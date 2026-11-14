import { useCallback, useEffect, useState } from "react";

// Altura del panel de eventos (spec web-front, "Panel de eventos intercambiados").
export const DEFAULT_HEIGHT = 340;
export const MIN_HEIGHT = 96; // barra del panel y cabecera de la tabla
const HEADER = 56; // fila de la cabecera de la pantalla (styles.css, .app)
export const SEPARATOR = 6;
const MIN_MAP = 160;
const STORAGE_KEY = "eda-retail.events-height";

export function maxHeight(viewport: number): number {
  return Math.max(MIN_HEIGHT, viewport - HEADER - SEPARATOR - MIN_MAP);
}

export function clampHeight(value: number, viewport: number): number {
  return Math.round(Math.min(maxHeight(viewport), Math.max(MIN_HEIGHT, value)));
}

// La altura elegida se guarda solo en este navegador; sin almacenamiento, se usa la de por defecto.
function stored(): number {
  try {
    const value = Number(window.localStorage.getItem(STORAGE_KEY));
    return Number.isFinite(value) && value > 0 ? value : DEFAULT_HEIGHT;
  } catch {
    return DEFAULT_HEIGHT;
  }
}

export function usePanelHeight() {
  const [viewport, setViewport] = useState(() => window.innerHeight);
  const [height, setRaw] = useState(() => clampHeight(stored(), window.innerHeight));

  useEffect(() => {
    const onResize = () => setViewport(window.innerHeight);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const setHeight = useCallback(
    (value: number) => {
      const next = clampHeight(value, viewport);
      setRaw(next);
      try {
        window.localStorage.setItem(STORAGE_KEY, String(next));
      } catch {
        // Sin almacenamiento la altura dura hasta recargar.
      }
    },
    [viewport],
  );

  return { height: clampHeight(height, viewport), setHeight, min: MIN_HEIGHT, max: maxHeight(viewport) };
}
