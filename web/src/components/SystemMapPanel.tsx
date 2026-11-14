import type { ApiClient } from "../api/client";
import { useTopology } from "../useTopology";
import type { HealthState } from "../useHealth";
import { type Highlight, SystemMap } from "./SystemMap";

interface Props {
  client: ApiClient;
  health: HealthState;
  highlight: Highlight | null;
  selected: string | null;
  onSelect: (nodeId: string) => void;
}

// Contenedor del mapa de sistemas: carga la topología y delega el dibujo en SystemMap.
export function SystemMapPanel({ client, ...props }: Props) {
  const topology = useTopology(client);

  return (
    <section className="map" aria-labelledby="map-title">
      <h2 id="map-title" className="panel-title">
        Mapa de sistemas
      </h2>
      {topology.kind === "ok" && <SystemMap topology={topology.topology} {...props} />}
      {topology.kind === "error" && <p className="map__error">No se pudo cargar la topología.</p>}
    </section>
  );
}
