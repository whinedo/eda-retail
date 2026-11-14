import { Fragment, useEffect, useMemo, useState } from "react";
import type { NodeKind, Topology, TopologyNode } from "../api/client";
import { MAP_HEIGHT, MAP_WIDTH, edgePath, layoutNodes } from "../mapLayout";
import type { HealthState } from "../useHealth";

// Comunicación en curso: emisor y destino del último evento; `seq` distingue eventos repetidos.
export interface Highlight {
  sender: string;
  receiver: string;
  seq: number;
}

export const HIGHLIGHT_MS = 3000;

type Edge = Topology["edges"][number];
type ProcessStatus = "up" | "down" | "unknown";

const edgeKey = (e: Edge) => `${e.source}->${e.target}`;

const LEGEND: { kind: NodeKind; text: string }[] = [
  { kind: "llm", text: "Agente con LLM" },
  { kind: "code", text: "Código" },
  { kind: "data", text: "Datos y bus" },
  { kind: "external", text: "Entrada o externo" },
  { kind: "simulated", text: "Servicio simulado" },
];

const STATUS_TEXT: Record<ProcessStatus, string> = {
  up: "Proceso activo",
  down: "Proceso caído",
  unknown: "Estado desconocido",
};

// Aristas del camino más corto de `from` a `to` (BFS). Primero respeta la dirección; si no hay
// camino dirigido, recorre las aristas sin dirección. null si no hay camino o un id no existe.
export function shortestPath(edges: readonly Edge[], from: string, to: string): Edge[] | null {
  return bfs(edges, from, to, false) ?? bfs(edges, from, to, true);
}

function bfs(edges: readonly Edge[], from: string, to: string, undirected: boolean): Edge[] | null {
  const next = new Map<string, { node: string; edge: Edge }[]>();
  const link = (a: string, b: string, edge: Edge) => {
    if (!next.has(a)) next.set(a, []);
    next.get(a)!.push({ node: b, edge });
  };
  for (const e of edges) {
    link(e.source, e.target, e);
    if (undirected) link(e.target, e.source, e);
  }
  const via = new Map<string, Edge | null>([[from, null]]);
  const queue = [from];
  while (queue.length > 0) {
    const current = queue.shift()!;
    if (current === to) break;
    for (const { node, edge } of next.get(current) ?? []) {
      if (via.has(node)) continue;
      via.set(node, edge);
      queue.push(node);
    }
  }
  if (!via.has(to)) return null;
  const path: Edge[] = [];
  let node = to;
  for (let edge = via.get(node); edge; edge = via.get(node)) {
    path.unshift(edge);
    node = edge.source === node ? edge.target : edge.source;
  }
  return path;
}

interface Active {
  edges: Set<string>;
  nodes: Set<string>;
}

// Resaltado de la comunicación en curso durante HIGHLIGHT_MS; cada evento nuevo lo reinicia.
function useActivePath(topology: Topology, highlight: Highlight | null): Active | null {
  const [active, setActive] = useState<Active | null>(null);
  const { sender, receiver, seq } = highlight ?? {};

  useEffect(() => {
    if (sender === undefined || receiver === undefined) return;
    const ids = new Set(topology.nodes.map((n) => n.id));
    const path = ids.has(sender) && ids.has(receiver) ? shortestPath(topology.edges, sender, receiver) : null;
    if (!path) {
      setActive(null);
      return;
    }
    setActive({ edges: new Set(path.map(edgeKey)), nodes: new Set([sender, receiver]) });
    const timer = setTimeout(() => setActive(null), HIGHLIGHT_MS);
    return () => clearTimeout(timer);
  }, [topology, sender, receiver, seq]);

  return active;
}

function processStatus(node: TopologyNode, health: HealthState): ProcessStatus | null {
  if (!node.monitored) return null;
  if (health.kind !== "ok") return "unknown";
  const up = health.health.processes[node.id];
  return up === undefined ? "unknown" : up ? "up" : "down";
}

export interface SystemMapProps {
  topology: Topology;
  health: HealthState;
  highlight: Highlight | null;
  selected: string | null;
  onSelect: (nodeId: string) => void;
}

// Presentacional: nodos, aristas, estado de procesos, resaltado y selección.
export function SystemMap({ topology, health, highlight, selected, onSelect }: SystemMapProps) {
  const boxes = useMemo(() => layoutNodes(topology.nodes.map((n) => n.id)), [topology]);
  const active = useActivePath(topology, highlight);

  return (
    <div className="system-map" style={{ width: MAP_WIDTH, height: MAP_HEIGHT }}>
      <svg className="system-map__edges" width={MAP_WIDTH} height={MAP_HEIGHT} aria-hidden="true">
        <defs>
          <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
            <path d="M 0 0 L 10 5 L 0 10 z" className="edge__head" />
          </marker>
          <marker
            id="arrow-active"
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" className="edge__head edge__head--active" />
          </marker>
        </defs>
        {topology.edges.map((edge) => {
          const from = boxes.get(edge.source);
          const to = boxes.get(edge.target);
          if (!from || !to) return null;
          const key = edgeKey(edge);
          const on = active?.edges.has(key) ?? false;
          return (
            <path
              key={key}
              data-edge={key}
              d={edgePath(from, to)}
              className={on ? "edge edge--active" : "edge"}
              markerEnd={on ? "url(#arrow-active)" : "url(#arrow)"}
            />
          );
        })}
      </svg>

      {topology.nodes.map((node) => {
        const box = boxes.get(node.id)!;
        const status = processStatus(node, health);
        const classes = ["node", `node--${node.kind}`];
        if (active?.nodes.has(node.id)) classes.push("node--active");
        if (selected === node.id) classes.push("node--selected");
        const statusId = `node-status-${node.id}`;
        return (
          <Fragment key={node.id}>
            <button
              type="button"
              className={classes.join(" ")}
              style={{ left: box.x, top: box.y, width: box.w, height: box.h }}
              aria-pressed={selected === node.id}
              aria-describedby={status ? statusId : undefined}
              onClick={() => onSelect(node.id)}
            >
              {node.label}
              {status && <span className={`node__dot node__dot--${status}`} aria-hidden="true" />}
            </button>
            {/* Fuera del botón para que el nombre accesible sea solo la etiqueta. */}
            {status && (
              <span id={statusId} className="visually-hidden">
                {STATUS_TEXT[status]}
              </span>
            )}
          </Fragment>
        );
      })}

      <div className="legend">
        <strong id="legend-title">Leyenda</strong>
        <ul aria-labelledby="legend-title">
          {LEGEND.map(({ kind, text }) => (
            <li key={kind}>
              <span className={`legend__swatch node--${kind}`} aria-hidden="true" />
              {text}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
