import { type CSSProperties, useCallback, useMemo, useState } from "react";
import { type ApiClient, createApiClient } from "./api/client";
import { CommandPanel } from "./components/CommandPanel";
import { EventsPanel } from "./components/EventsPanel";
import { Header } from "./components/Header";
import { NodeDetailPanel } from "./components/NodeDetail";
import { PanelResizer } from "./components/PanelResizer";
import type { Highlight } from "./components/SystemMap";
import { SystemMapPanel } from "./components/SystemMapPanel";
import { type OpenStream, openEventSource } from "./useCommandRun";
import { useConfig } from "./useConfig";
import { useHealth } from "./useHealth";
import { useLiveEvents } from "./useLiveEvents";
import { usePanelHeight } from "./usePanelHeight";

// `openStream` debe ser estable entre renders: cambiarlo reabre el stream de eventos.
export default function App({ client, openStream = openEventSource }: { client?: ApiClient; openStream?: OpenStream }) {
  const api = useMemo(() => client ?? createApiClient(), [client]);
  const health = useHealth(api);
  const { jaegerUrl, freeText, interactive } = useConfig(api);
  const events = useLiveEvents(api, openStream);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [commandRunning, setCommandRunning] = useState(false);
  const panel = usePanelHeight();
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
      <Header health={health} jaegerUrl={jaegerUrl} onReset={resetView} resetDisabled={commandRunning} />
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
  );
}
