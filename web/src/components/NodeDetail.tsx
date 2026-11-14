import type { ApiClient, NodeDetail as NodeDetailData, NodeKind } from "../api/client";
import { type NodeDetailState, useNodeDetail } from "../useNodeDetail";

const KIND_LABEL: Record<NodeKind, string> = {
  llm: "Agente con LLM",
  code: "Código",
  data: "Datos y bus",
  external: "Entrada o externo",
  simulated: "Servicio simulado",
};

function processState(up: boolean | null): { text: string; className: string } {
  if (up === null) return { text: "sin proceso propio", className: "pill--pending" };
  return up ? { text: "activo", className: "pill--ok" } : { text: "caído", className: "pill--down" };
}

function formatLatency(ms: number): string {
  return `${(ms / 1000).toLocaleString("es-ES", { minimumFractionDigits: 1, maximumFractionDigits: 1 })} s`;
}

function TypeList({ title, types }: { title: string; types: string[] }) {
  return (
    <div>
      <dt>{title}</dt>
      <dd>{types.length ? types.map((type) => <code key={type}>{type}</code>) : "—"}</dd>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function NodeBody({ node, onFilter }: { node: NodeDetailData; onFilter(): void }) {
  const status = processState(node.up);
  const number = (n: number) => n.toLocaleString("es-ES");
  return (
    <>
      <p className="drawer__status">
        <span className={`pill ${status.className}`}>
          <span className="pill__dot" />
          {status.text}
        </span>
      </p>
      <p className="drawer__description">{node.description}</p>
      <dl className="drawer__facts">
        <TypeList title="Consume" types={node.consumes} />
        <TypeList title="Produce" types={node.emits} />
      </dl>
      {node.llm && (
        <div className="drawer__metrics">
          <Metric label="Llamadas" value={number(node.llm.calls)} />
          <Metric label="Tokens de entrada" value={number(node.llm.tokens_in)} />
          <Metric label="Tokens de salida" value={number(node.llm.tokens_out)} />
          <Metric label="Latencia media" value={formatLatency(node.llm.avg_latency_ms)} />
          <Metric label="Validaciones superadas" value={number(node.llm.validations_passed)} />
        </div>
      )}
      <button type="button" className="primary" onClick={onFilter}>
        Filtrar eventos
      </button>
    </>
  );
}

export interface NodeDetailProps {
  nodeId: string;
  state: NodeDetailState;
  onFilter(nodeId: string): void;
  onClose(): void;
}

// Panel lateral con el detalle de un nodo del mapa: presentacional.
export function NodeDetail({ nodeId, state, onFilter, onClose }: NodeDetailProps) {
  const title = state.kind === "ok" ? state.node.label : nodeId;
  return (
    <aside className="drawer" aria-labelledby="drawer-title">
      <header className="drawer__header">
        <h2 id="drawer-title" className="panel-title">
          {title}
        </h2>
        {state.kind === "ok" && <span className={`badge badge--${state.node.kind}`}>{KIND_LABEL[state.node.kind]}</span>}
        <button type="button" className="drawer__close" aria-label="Cerrar detalle" onClick={onClose}>
          ×
        </button>
      </header>
      {state.kind === "loading" && <p className="drawer__note">Cargando…</p>}
      {state.kind === "not_found" && <p className="drawer__note">Nodo desconocido: {nodeId}</p>}
      {state.kind === "error" && <p className="drawer__note">No se pudo cargar el detalle del nodo</p>}
      {state.kind === "ok" && <NodeBody node={state.node} onFilter={() => onFilter(state.node.id)} />}
    </aside>
  );
}

// Contenedor: carga el detalle del nodo seleccionado.
export function NodeDetailPanel({ client, nodeId, onFilter, onClose }: Omit<NodeDetailProps, "state"> & { client: ApiClient }) {
  const state = useNodeDetail(client, nodeId);
  return <NodeDetail nodeId={nodeId} state={state} onFilter={onFilter} onClose={onClose} />;
}
