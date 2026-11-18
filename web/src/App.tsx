import { type CSSProperties, useCallback, useEffect, useMemo, useState } from "react";
import { type ApiClient, createApiClient } from "./api/client";
import { CommandPanel } from "./components/CommandPanel";
import { EventsPanel } from "./components/EventsPanel";
import { Header } from "./components/Header";
import { NodeDetailPanel } from "./components/NodeDetail";
import { OperationsView } from "./components/OperationsView";
import { OrderAnalytics } from "./components/OrderAnalytics";
import { PanelResizer } from "./components/PanelResizer";
import { ProfitView } from "./components/ProfitView";
import { ParkedNoticesBanner } from "./components/ParkedNoticesBanner";
import type { Highlight } from "./components/SystemMap";
import { SystemMapPanel } from "./components/SystemMapPanel";
import { type OpenStream, openEventSource } from "./useCommandRun";
import { useConfig } from "./useConfig";
import { useHealth } from "./useHealth";
import { useLiveEvents } from "./useLiveEvents";
import { useOperations } from "./useOperations";
import { useOrderAnalytics } from "./useOrderAnalytics";
import { usePanelHeight } from "./usePanelHeight";
import { useParkedNotices } from "./useParkedNotices";
import { useProfit } from "./useProfit";

export type Route = "main" | "orders" | "operations" | "profit";
const HASHES: Record<Route, string> = {
  main: "#/",
  orders: "#/analytics",
  operations: "#/analytics/operations",
  profit: "#/analytics/operations/profit",
};

function routeOf(hash: string): Route {
  if (hash === HASHES.profit) return "profit";
  if (hash === HASHES.operations) return "operations";
  return hash === HASHES.orders ? "orders" : "main";
}

// Vista abierta según el hash de la URL: la principal (auditoría), la de analítica de pedidos, la de operaciones o la
// de beneficio.
function useRoute(): [Route, (next: Route) => void] {
  const [route, setRoute] = useState(() => routeOf(window.location.hash));
  useEffect(() => {
    const sync = () => setRoute(routeOf(window.location.hash));
    window.addEventListener("hashchange", sync);
    return () => window.removeEventListener("hashchange", sync);
  }, []);
  const navigate = useCallback((next: Route) => {
    window.location.hash = HASHES[next];
    setRoute(next);
  }, []);
  return [route, navigate];
}

// `openStream` debe ser estable entre renders: cambiarlo reabre el stream de eventos.
export default function App({ client, openStream = openEventSource }: { client?: ApiClient; openStream?: OpenStream }) {
  const api = useMemo(() => client ?? createApiClient(), [client]);
  const health = useHealth(api);
  const { jaegerUrl, freeText, interactive, pocVersion } = useConfig(api);
  const events = useLiveEvents(api, openStream);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [commandRunning, setCommandRunning] = useState(false);
  const panel = usePanelHeight();
  const [route, navigate] = useRoute();
  const analyticsOpen = route !== "main";
  const analytics = useOrderAnalytics(api, route === "orders");
  const operations = useOperations(api, route === "operations");
  const profit = useProfit(api, route === "profit");
  const parkedNotices = useParkedNotices(api);
  const [resetKey, setResetKey] = useState(0);
  const { reset: resetEvents } = events;
  // Reinicio de la vista: solo estado del front; no se escribe nada en el backend.
  const resetView = useCallback(() => {
    resetEvents();
    setSelectedNode(null);
    setResetKey((key) => key + 1);
  }, [resetEvents]);
  // Comunicación en curso: el último evento recibido en vivo resalta su camino en el mapa.
  const last = events.lastEvent;
  const highlight = useMemo<Highlight | null>(
    () => (last ? { sender: last.sender, receiver: last.receiver, seq: last.seq } : null),
    [last],
  );

  return (
    <div className="app">
      <Header
        health={health}
        jaegerUrl={jaegerUrl}
        pocVersion={pocVersion}
        onReset={resetView}
        resetDisabled={commandRunning}
        analyticsOpen={analyticsOpen}
        onAnalytics={() => navigate("orders")}
      />
      {/* Visible en las dos vistas mientras el intérprete tenga avisos aparcados. */}
      <ParkedNoticesBanner notices={parkedNotices} />
      {route === "operations" && (
        <OperationsView
          data={operations.data}
          error={operations.error}
          state={operations.state}
          reason={operations.reason}
          incidentId={operations.incidentId}
          paused={operations.paused}
          onStateChange={operations.setState}
          onReasonChange={operations.setReason}
          onIncidentChange={operations.setIncidentId}
          onTogglePause={operations.paused ? operations.resume : operations.pause}
          onLoadMore={operations.loadMore}
          onViewEvents={(incidentId) => {
            events.setIncidentId(incidentId);
            navigate("main");
          }}
          onOrders={() => navigate("orders")}
          onProfit={() => navigate("profit")}
          onBack={() => navigate("main")}
        />
      )}
      {route === "profit" && (
        <ProfitView
          data={profit.data}
          error={profit.error}
          onOperations={() => navigate("operations")}
          onBack={() => navigate("main")}
        />
      )}
      {route === "orders" && (
        <OrderAnalytics
          data={analytics.data}
          error={analytics.error}
          status={analytics.status}
          incidentId={analytics.incidentId}
          paused={analytics.paused}
          onStatusChange={analytics.setStatus}
          onIncidentChange={analytics.setIncidentId}
          onTogglePause={analytics.paused ? analytics.resume : analytics.pause}
          onOperations={() => navigate("operations")}
          onBack={() => navigate("main")}
        />
      )}
      {/* La vista principal sigue montada al abrir la analítica: conserva filtros, altura y la salida del comando. */}
      <div className="app__view" hidden={analyticsOpen}>
        <CommandPanel client={api} openStream={openStream} freeText={freeText} interactive={interactive} resetKey={resetKey} onRunningChange={setCommandRunning} />
        <main className="main" style={{ "--events-height": `${panel.height}px` } as CSSProperties}>
          <SystemMapPanel
            client={api}
            health={health}
            highlight={highlight}
            selected={selectedNode}
            onSelect={setSelectedNode}
          />
          {selectedNode && (
            <NodeDetailPanel
              client={api}
              nodeId={selectedNode}
              onFilter={events.setNode}
              onClose={() => setSelectedNode(null)}
            />
          )}
          <PanelResizer height={panel.height} min={panel.min} max={panel.max} onChange={panel.setHeight} />
          <EventsPanel
            rows={events.rows}
            paused={events.paused}
            incidentId={events.incidentId}
            node={events.node}
            hasMore={events.hasMore}
            error={events.error}
            jaegerUrl={jaegerUrl}
            onIncidentChange={events.setIncidentId}
            onClearNode={() => events.setNode(null)}
            onTogglePause={events.paused ? events.resume : events.pause}
            onLoadMore={() => void events.loadMore()}
          />
        </main>
      </div>
    </div>
  );
}
