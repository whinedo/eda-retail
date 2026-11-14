import { useEffect, useState } from "react";
import { type ApiClient, ApiError, type NodeDetail } from "./api/client";

export type NodeDetailState =
  | { kind: "loading" }
  | { kind: "ok"; node: NodeDetail }
  | { kind: "not_found" }
  | { kind: "error" };

// Contenedor: pide /api/nodes/{id} cada vez que cambia el nodo seleccionado.
export function useNodeDetail(client: ApiClient, nodeId: string): NodeDetailState {
  const [state, setState] = useState<NodeDetailState>({ kind: "loading" });

  useEffect(() => {
    let active = true;
    setState({ kind: "loading" });
    client.getNode(nodeId).then(
      (node) => active && setState({ kind: "ok", node }),
      (error) => active && setState(error instanceof ApiError && error.status === 404 ? { kind: "not_found" } : { kind: "error" }),
    );
    return () => {
      active = false;
    };
  }, [client, nodeId]);

  return state;
}
