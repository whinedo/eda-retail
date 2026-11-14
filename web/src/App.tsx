import { useMemo } from "react";
import { type ApiClient, createApiClient } from "./api/client";
import { CommandMenu } from "./components/CommandMenu";
import { EventsPanel } from "./components/EventsPanel";
import { Header } from "./components/Header";
import { SystemMap } from "./components/SystemMap";
import { useHealth } from "./useHealth";

export default function App({ client }: { client?: ApiClient }) {
  const api = useMemo(() => client ?? createApiClient(), [client]);
  const health = useHealth(api);

  return (
    <div className="app">
      <Header health={health} />
      <CommandMenu />
      <main className="main">
        <SystemMap />
        <EventsPanel />
      </main>
    </div>
  );
}
