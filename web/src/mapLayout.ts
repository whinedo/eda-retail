// Posiciones del mapa de sistemas (mockups docs/front/1-vista-general.png) por id de nodo de
// GET /api/topology, y trazado ortogonal de las aristas entre ellas. Sin dependencias de React.

export interface Box {
  x: number;
  y: number;
  w: number;
  h: number;
}

export const MAP_WIDTH = 1240;
export const MAP_HEIGHT = 650;
// Línea común por la que los consumidores escriben en PostgreSQL (command handler).
const BUS_Y = 580;

const CONSUMERS_X = 790;
const CONSUMERS_W = 190;
const RIGHT_X = 1050;
const RIGHT_W = 180;

const executor = (action: string, y: number): [string, Box] => [
  `executor-${action}`,
  { x: CONSUMERS_X, y, w: CONSUMERS_W, h: 36 },
];
const agent = (id: string, y: number): [string, Box] => [id, { x: CONSUMERS_X, y, w: CONSUMERS_W, h: 44 }];
const service = (id: string, y: number, h = 40): [string, Box] => [id, { x: RIGHT_X, y, w: RIGHT_W, h }];

export const POSITIONS: Readonly<Record<string, Box>> = Object.fromEntries([
  ["projectors", { x: 200, y: 30, w: 160, h: 64 }],
  ["intake", { x: 0, y: 222, w: 140, h: 56 }],
  ["postgres", { x: 190, y: 200, w: 170, h: 100 }],
  ["relay", { x: 410, y: 220, w: 110, h: 60 }],
  ["dlq", { x: 410, y: 350, w: 110, h: 56 }],
  ["kafka", { x: 570, y: 205, w: 150, h: 90 }],
  agent("interpreter", 20),
  agent("planner", 75),
  agent("replanner", 130),
  agent("orchestrator", 185),
  executor("notify_customer", 260),
  executor("file_claim", 306),
  executor("reissue_shipment", 352),
  executor("reserve_stock", 398),
  executor("release_stock", 444),
  executor("issue_refund", 490),
  service("ollama", 20, 154),
  service("mail", 258),
  service("claims", 304),
  service("carrier", 350),
  service("warehouse", 398, 82),
  service("payments", 488),
  service("jaeger", 586, 52),
]);

// Zona libre (abajo, entre la leyenda y la línea común) para nodos que la tabla no conoce.
const FALLBACK = { x: 190, y: 440, w: 130, h: 44, gap: 14, perRow: 4 };

// Posición de cada nodo: la de la tabla o, si es nuevo, una casilla libre de la zona de reserva.
export function layoutNodes(ids: readonly string[]): Map<string, Box> {
  const boxes = new Map<string, Box>();
  let extra = 0;
  for (const id of ids) {
    const known = POSITIONS[id];
    if (known) {
      boxes.set(id, known);
      continue;
    }
    const col = extra % FALLBACK.perRow;
    const row = Math.floor(extra / FALLBACK.perRow);
    boxes.set(id, {
      x: FALLBACK.x + col * (FALLBACK.w + FALLBACK.gap),
      y: FALLBACK.y + row * (FALLBACK.h + FALLBACK.gap),
      w: FALLBACK.w,
      h: FALLBACK.h,
    });
    extra += 1;
  }
  return boxes;
}

const cx = (b: Box) => b.x + b.w / 2;
const cy = (b: Box) => b.y + b.h / 2;
const right = (b: Box) => b.x + b.w;
const bottom = (b: Box) => b.y + b.h;
const clamp = (v: number, min: number, max: number) => Math.min(Math.max(v, min), max);

// Trazado SVG (atributo d) de la arista de `from` a `to`, en tramos horizontales y verticales.
export function edgePath(from: Box, to: Box): string {
  const overlapX = Math.min(right(from), right(to)) - Math.max(from.x, to.x);
  if (overlapX > 0) {
    // Uno encima del otro: tramo vertical en el centro de la parte común.
    const x = (Math.max(from.x, to.x) + Math.min(right(from), right(to))) / 2;
    return to.y > from.y ? `M ${x} ${bottom(from)} V ${to.y}` : `M ${x} ${from.y} V ${bottom(to)}`;
  }
  if (to.x >= right(from)) {
    const y = cy(from);
    if (y >= to.y + 4 && y <= bottom(to) - 4) return `M ${right(from)} ${y} H ${to.x}`;
    // Codo antes del destino: los consumidores de Kafka comparten tronco vertical.
    const mx = to.x - 20;
    const ty = clamp(y, to.y + 10, bottom(to) - 10);
    return `M ${right(from)} ${y} H ${mx} V ${ty} H ${to.x}`;
  }
  // Hacia la izquierda: baja a la línea común y sube al destino por debajo.
  const sx = from.x - 10;
  return `M ${from.x} ${cy(from)} H ${sx} V ${BUS_Y} H ${cx(to)} V ${bottom(to)}`;
}
