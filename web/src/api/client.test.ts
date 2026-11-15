import { describe, expect, it, vi } from "vitest";
import { ApiError, commandStreamUrl, createApiClient, eventsStreamUrl } from "./client";

function fakeFetch(status: number, body: unknown) {
  return vi.fn(async (_input: string, _init?: RequestInit) =>
    new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } }),
  );
}

describe("API client", () => {
  it("reads the health report", async () => {
    const health = {
      infra: { postgres: "healthy", kafka: "down", ollama: "healthy" },
      processes: { relay: true },
      processes_up: 1,
      processes_total: 12,
    };
    const fetchFn = fakeFetch(200, health);

    expect(await createApiClient(fetchFn).getHealth()).toEqual(health);
    expect(fetchFn).toHaveBeenCalledWith("/api/health", expect.objectContaining({ method: "GET" }));
  });

  it("builds the events query and leaves out empty filters", async () => {
    const fetchFn = fakeFetch(200, { events: [], next_before_seq: null });

    await createApiClient(fetchFn).getEvents({ limit: 50, beforeSeq: 1290, incidentId: "INC-0007" });
    await createApiClient(fetchFn).getEvents({ node: "executor-notify_customer" });

    expect(fetchFn.mock.calls[0][0]).toBe("/api/events?limit=50&before_seq=1290&incident_id=INC-0007");
    expect(fetchFn.mock.calls[1][0]).toBe("/api/events?node=executor-notify_customer");
  });

  it("builds the order analytics query and leaves out empty filters", async () => {
    const fetchFn = fakeFetch(200, { counts: {}, orders: [], applied: {} });

    await createApiClient(fetchFn).getOrderAnalytics({ status: "delayed", incidentId: "INC-0007" });
    await createApiClient(fetchFn).getOrderAnalytics();

    expect(fetchFn.mock.calls[0][0]).toBe("/api/analytics/orders?status=delayed&incident_id=INC-0007");
    expect(fetchFn.mock.calls[1][0]).toBe("/api/analytics/orders");
  });

  it("encodes the node id", async () => {
    const fetchFn = fakeFetch(200, {});

    await createApiClient(fetchFn).getNode("a/b c");

    expect(fetchFn.mock.calls[0][0]).toBe("/api/nodes/a%2Fb%20c");
  });

  it("posts a command as JSON", async () => {
    const fetchFn = fakeFetch(202, { run_id: "r1", argv: ["scripts/notice.sh", "send", "--example", "loss"] });

    const run = await createApiClient(fetchFn).runCommand({ command: "send", example: "loss" });

    expect(run.run_id).toBe("r1");
    const [url, init] = fetchFn.mock.calls[0];
    expect(url).toBe("/api/commands");
    expect(init?.method).toBe("POST");
    expect(new Headers(init?.headers).get("Content-Type")).toBe("application/json");
    expect(JSON.parse(String(init?.body))).toEqual({ command: "send", example: "loss" });
  });

  it("sends an input line to a run", async () => {
    const fetchFn = fakeFetch(202, { ok: true });

    await createApiClient(fetchFn).sendInput("r1", "2");

    expect(fetchFn.mock.calls[0][0]).toBe("/api/commands/r1/input");
    expect(JSON.parse(String(fetchFn.mock.calls[0][1]?.body))).toEqual({ line: "2" });
  });

  it("turns an error response into an ApiError with the backend detail", async () => {
    const fetchFn = fakeFetch(409, { detail: "ya hay un comando en ejecución" });

    const error = await createApiClient(fetchFn).runCommand({ command: "seed" }).catch((e) => e);

    expect(error).toBeInstanceOf(ApiError);
    expect(error.status).toBe(409);
    expect(error.message).toBe("ya hay un comando en ejecución");
  });

  it("builds the SSE URLs", () => {
    expect(eventsStreamUrl(1290)).toBe("/api/events/stream?after_seq=1290");
    expect(eventsStreamUrl()).toBe("/api/events/stream");
    expect(commandStreamUrl("r 1")).toBe("/api/commands/r%201/stream");
  });

  it("reads the front config", async () => {
    const fetchFn = fakeFetch(200, { jaeger_ui_url: "http://localhost:16686" });

    expect(await createApiClient(fetchFn).getConfig()).toEqual({ jaeger_ui_url: "http://localhost:16686" });
    expect(fetchFn.mock.calls[0][0]).toBe("/api/config");
  });
});
