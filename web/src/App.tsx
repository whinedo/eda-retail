import { useMemo, useState } from "react";
import { type ApiClient, createApiClient } from "./api/client";
import { CommandPanel } from "./components/CommandPanel";
import { EventsPanel } from "./components/EventsPanel";
import { Header } from "./components/Header";
import { NodeDetailPanel } from "./components/NodeDetail";
import { SystemMap } from "./components/SystemMap";
import { type OpenStream, openEventSource } from "./useCommandRun";
import { useHealth } from "./useHealth";
import { useLiveEvents } from "./useLiveEvents";

// `openStream` debe ser estable entre renders: cambiarlo reabre el stream de eventos.
export default function App({ client, openStream = openEventSource }: { client?: ApiClient; openStream?: OpenStream }) {
  const api = useMemo(() => client ?? createApiClient(), [client]);
  const health = useHealth(api);
  const events = useLiveEvents(api, openStream);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  return (
    <div className="app">
      <Header health={health} />
      <CommandPanel client={api} openStream={openStream} />
      <main className="main">
        <SystemMap />
        {selectedNode && (
          <NodeDetailPanel
            client={api}
            nodeId={selectedNode}
            onFilter={events.setNode}
            onClose={() => setSelectedNode(null)}
          />
        )}
        <EventsPanel
          rows={events.rows}
          paused={events.paused}
          incidentId={events.incidentId}
          node={events.node}
          hasMore={events.hasMore}
          error={events.error}
          onIncidentChange={events.setIncidentId}
          onClearNode={() => events.setNode(null)}
          onTogglePause={events.paused ? events.resume : events.pause}
          onLoadMore={() => void events.loadMore()}
        />
      </main>
    </div>
  );
}
