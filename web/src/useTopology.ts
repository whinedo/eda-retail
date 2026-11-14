import { useEffect, useState } from "react";
import type { ApiClient, Topology } from "./api/client";

export type TopologyState = { kind: "loading" } | { kind: "ok"; topology: Topology } | { kind: "error" };

// Contenedor: la topología es estática, así que se pide una sola vez al montar.
export function useTopology(client: ApiClient): TopologyState {
  const [state, setState] = useState<TopologyState>({ kind: "loading" });

  useEffect(() => {
    let active = true;
    client.getTopology().then(
      (topology) => active && setState({ kind: "ok", topology }),
      () => active && setState({ kind: "error" }),
    );
    return () => {
      active = false;
    };
  }, [client]);

  return state;
}
