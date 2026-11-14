import type { HealthState } from "../useHealth";

function Pill({ ok, children }: { ok: boolean | null; children: string }) {
  const tone = ok === null ? "pending" : ok ? "ok" : "down";
  return (
    <li className={`pill pill--${tone}`}>
      <span className="pill__dot" aria-hidden="true" />
      {children}
    </li>
  );
}

// Presentacional: estado de la infraestructura y de los procesos.
export function Header({ health }: { health: HealthState }) {
  return (
    <header className="header">
      <div className="header__title">
        <strong>EDA Retail</strong>
        <span>Incidencias logísticas · PoC académica</span>
      </div>
      <ul className="header__status" aria-label="Estado de la infraestructura">
        {health.kind === "loading" && <Pill ok={null}>Comprobando…</Pill>}
        {health.kind === "offline" && <Pill ok={false}>Backend: sin conexión</Pill>}
        {health.kind === "ok" && (
          <>
            <Pill ok={health.health.infra.kafka === "healthy"}>{`Kafka: ${health.health.infra.kafka}`}</Pill>
            <Pill ok={health.health.infra.postgres === "healthy"}>
              {`PostgreSQL: ${health.health.infra.postgres}`}
            </Pill>
            <Pill ok={health.health.infra.ollama === "healthy"}>{`Ollama: ${health.health.infra.ollama}`}</Pill>
            <Pill ok={health.health.processes_up === health.health.processes_total}>
              {`Procesos: ${health.health.processes_up} de ${health.health.processes_total}`}
            </Pill>
          </>
        )}
      </ul>
    </header>
  );
}
