import { useMemo, useState } from "react";
import { type ApiClient, createApiClient } from "./api/client";
import { CommandPanel } from "./components/CommandPanel";
import { EventsPanel } from "./components/EventsPanel";
import { Header } from "./components/Header";
import { SystemMapPanel } from "./components/SystemMapPanel";
import { useHealth } from "./useHealth";

export default function App({ client }: { client?: ApiClient }) {
  const api = useMemo(() => client ?? createApiClient(), [client]);
  const health = useHealth(api);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  return (
    <div className="app">
      <Header health={health} />
      <CommandPanel client={api} />
      <main className="main">
        <SystemMapPanel
          client={api}
          health={health}
          highlight={null}
          selected={selectedNode}
          onSelect={setSelectedNode}
        />
        <EventsPanel />
      </main>
    </div>
  );
}
