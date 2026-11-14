import type { KeyboardEvent, MouseEvent } from "react";

const STEP = 20;

interface Props {
  height: number;
  min: number;
  max: number;
  onChange: (height: number) => void;
}

// Presentacional: separador horizontal entre el mapa y el panel de eventos. Arrastrarlo hacia
// abajo encoge el panel; con el foco, las flechas cambian 20 px e Inicio/Fin van a los extremos.
export function PanelResizer({ height, min, max, onChange }: Props) {
  const startDrag = (event: MouseEvent) => {
    event.preventDefault(); // sin selección de texto mientras se arrastra
    const startY = event.clientY;
    const startHeight = height;
    const move = (e: globalThis.MouseEvent) => onChange(startHeight - (e.clientY - startY));
    const stop = () => {
      window.removeEventListener("mousemove", move);
      window.removeEventListener("mouseup", stop);
      document.body.classList.remove("resizing");
    };
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", stop);
    document.body.classList.add("resizing");
  };

  const onKeyDown = (event: KeyboardEvent) => {
    const next: Record<string, number> = {
      ArrowUp: height + STEP,
      ArrowDown: height - STEP,
      Home: min,
      End: max,
    };
    if (!(event.key in next)) return;
    event.preventDefault();
    onChange(next[event.key]);
  };

  return (
    <div
      className="resizer"
      role="separator"
      aria-label="Altura del panel de eventos"
      aria-orientation="horizontal"
      aria-valuenow={height}
      aria-valuemin={min}
      aria-valuemax={max}
      tabIndex={0}
      title="Arrastra para cambiar la altura del panel de eventos"
      onMouseDown={startDrag}
      onKeyDown={onKeyDown}
    />
  );
}
