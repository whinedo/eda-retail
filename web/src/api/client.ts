// Cliente tipado de la API del backend (src/eda_retail/web). Mismo origen: en desarrollo,
// Vite redirige /api a eda-web en 127.0.0.1:8000.

export type Status = "healthy" | "down";

export interface Health {
  infra: { postgres: Status; kafka: Status; ollama: Status };
  processes: Record<string, boolean>;
  processes_up: number;
  processes_total: number;
}

export type NodeKind = "llm" | "code" | "data" | "external" | "simulated";

export interface TopologyNode {
  id: string;
  label: string;
  kind: NodeKind;
  description: string;
  monitored: boolean;
}

export interface Topology {
  nodes: TopologyNode[];
  edges: { source: string; target: string }[];
}

export interface EventRow {
  seq: number;
  event_id: string;
  event_type: string;
  occurred_at: string;
  sender: string;
  receiver: string;
  receivers: string[];
  incident_id: string;
  plan_version: number | null;
  partition: number | null;
  offset: number | null;
  causation_id: string | null;
  payload: Record<string, unknown>;
  trace_id: string | null;
}

export interface EventsPage {
  events: EventRow[];
  next_before_seq: number | null;
}

export interface EventsQuery {
  limit?: number;
  beforeSeq?: number;
  incidentId?: string;
  node?: string;
}

export interface LlmMetrics {
  calls: number;
  tokens_in: number;
  tokens_out: number;
  avg_latency_ms: number;
  validations_passed: number;
}

export interface NodeDetail extends TopologyNode {
  up: boolean | null;
  consumes: string[];
  emits: string[];
  llm: LlmMetrics | null;
}

// Vista de analítica de pedidos (spec web-front): GET /api/analytics/orders, solo lee order_status.
export type OrderStatus = "delayed" | "in_resolution" | "resolved" | "refunded" | "escalated";

export interface OrderStatusRow {
  order_id: string;
  incident_id: string;
  status: OrderStatus;
  last_event_type: string;
  last_event_id: string;
  occurred_at: string;
  kafka_partition: number;
  kafka_offset: number;
}

export interface OrderAnalytics {
  counts: Record<OrderStatus, number>;
  orders: OrderStatusRow[];
  applied: Record<string, number>;
}

export interface OrderAnalyticsQuery {
  status?: OrderStatus;
  incidentId?: string;
}

// Aviso de avisos aparcados (spec web-front): GET /api/parked-notices, solo lee parked_notices.
export interface ParkedNotice {
  incident_id: string;
  order_ids: string[];
  deadline_at: string;
  parked_at: string;
  last_event_id: string;
  kafka_partition: number;
  kafka_offset: number;
}

export interface ParkedNotices {
  notices: ParkedNotice[];
  applied: Record<string, number>;
}

// Vista de operaciones (spec operations-view): GET /api/operations/*, solo lee las vistas de incident-metrics.
export type IncidentState = "open" | "resolved" | "refunded" | "escalated";
// Los importes llegan como texto (Decimal en el backend).
type Money = string | number;

export interface OperationsSummary {
  by_state: Record<IncidentState, number>;
  escalations: { reason: string; count: number }[];
  by_plan_version: Record<string, number>;
  step_failures: { action: string; error_code: string; count: number }[];
  cost_by_scenario: {
    cause: string | null;
    severity: string | null;
    incidents: number;
    mean_tokens: number;
    llm_cost: Money;
    effects_cost: Money;
    refunded: Money;
  }[];
  prices_configured: boolean;
  applied: Record<string, number>;
}

export interface IncidentMetricsRow {
  incident_id: string;
  carrier: string;
  cause: string | null;
  severity: string | null;
  order_count: number;
  state: IncidentState;
  escalation_reason: string | null;
  plan_version: number;
  steps_failed: number;
  compensations: number;
  opened_at: string;
  closed_at: string | null;
  duration_s: number | null;
  tokens: number;
  llm_cost: Money;
  effects_cost: Money;
  refunded: Money;
  last_event_at: string;
  last_event_type: string;
  last_event_id: string;
  kafka_partition: number;
  kafka_offset: number;
}

export interface IncidentMetricsPage {
  incidents: IncidentMetricsRow[];
  applied: Record<string, number>;
  next_before: string | null;
}

export interface IncidentMetricsQuery {
  state?: IncidentState;
  reason?: string;
  incidentId?: string;
  before?: string;
}

export interface LlmAgentStats {
  agent: string;
  calls: number;
  tokens_in: number;
  tokens_out: number;
  latency_mean_ms: number | null;
  latency_p95_ms: number | null;
  validated_pct: number | null;
}

export interface LlmStats {
  agents: LlmAgentStats[];
  applied: Record<string, number>;
}

export interface CarrierOperations {
  carriers: { carrier: string; incidents: number; delays: number; claims_filed: number; refunds_issued: number }[];
  consults: { outcome: string; consults: number; mean_rounds: number; mean_latency_ms: number }[];
  applied: Record<string, number>;
}

export interface PartitionLag {
  partition: number;
  applied: number | null;
  end_offset: number | null;
  lag: number | null;
}

export interface Freshness {
  consumers: { consumer: string; partitions: PartitionLag[]; lag: number | null }[];
}

// Beneficio total (spec incident-metrics, «Consultas de operaciones»): importes en euros, como texto decimal.
export interface Profit {
  revenue: string | number;
  shipping: string | number;
  reissue_shipping: string | number;
  reissue_fixed: string | number;
  refunds: string | number;
  claims: string | number;
  llm: string | number;
  profit: string | number;
  shipping_pct: string | number;
  applied: Record<string, number>;
}

export type CommandRequest =
  | { command: "seed" }
  | { command: "send"; example?: string; text?: string }
  | { command: "interactive"; timeout?: number };

export interface CommandRun {
  run_id: string;
  argv: string[];
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export interface FrontConfig {
  jaeger_ui_url: string;
  free_text_enabled: boolean;
  interactive_enabled: boolean;
  // Versión de la PoC con la que corre el backend (POC_VERSION): 1 o 2.
  poc_version?: number;
  // Soporte técnico (WEB_SUPPORT) y los argumentos que admite su lista blanca.
  support_enabled?: boolean;
  support_components?: string[];
  support_views?: string[];
}

// Una petición de soporte técnico (spec web-front, «Soporte técnico»): la lista blanca de `eda-support`.
export type SupportRequest =
  | { action: "status" }
  | { action: "republish"; mode: "truncated" | "recreated"; confirm?: string }
  | { action: "replay-from"; component: string; outbox_id: number; confirm?: string }
  | { action: "rebuild"; view: string; confirm?: string };

export interface ApiClient {
  getHealth(): Promise<Health>;
  getConfig(): Promise<FrontConfig>;
  getTopology(): Promise<Topology>;
  getEvents(query?: EventsQuery): Promise<EventsPage>;
  getNode(id: string): Promise<NodeDetail>;
  getOrderAnalytics(query?: OrderAnalyticsQuery): Promise<OrderAnalytics>;
  getParkedNotices(): Promise<ParkedNotices>;
  getOperationsSummary(incidentId?: string): Promise<OperationsSummary>;
  getOperationsIncidents(query?: IncidentMetricsQuery): Promise<IncidentMetricsPage>;
  getOperationsLlm(): Promise<LlmStats>;
  getOperationsCarriers(): Promise<CarrierOperations>;
  getOperationsFreshness(): Promise<Freshness>;
  getOperationsProfit(): Promise<Profit>;
  runCommand(request: CommandRequest): Promise<CommandRun>;
  runSupport(request: SupportRequest): Promise<CommandRun>;
  sendInput(runId: string, line: string): Promise<void>;
}

type FetchFn = (input: string, init?: RequestInit) => Promise<Response>;

export function createApiClient(fetchFn: FetchFn = (input, init) => fetch(input, init)): ApiClient {
  async function call<T>(method: "GET" | "POST", path: string, body?: unknown): Promise<T> {
    const init: RequestInit = { method };
    if (body !== undefined) {
      init.headers = { "Content-Type": "application/json" };
      init.body = JSON.stringify(body);
    }
    const response = await fetchFn(path, init);
    const data = await response.json().catch(() => null);
    if (!response.ok) {
      const detail = typeof data?.detail === "string" ? data.detail : validationDetail(data?.detail) ?? `HTTP ${response.status}`;
      throw new ApiError(response.status, detail);
    }
    return data as T;
  }

  return {
    getHealth: () => call("GET", "/api/health"),
    getConfig: () => call("GET", "/api/config"),
    getTopology: () => call("GET", "/api/topology"),
    getEvents: (query = {}) => call("GET", `/api/events${eventsQueryString(query)}`),
    getNode: (id) => call("GET", `/api/nodes/${encodeURIComponent(id)}`),
    getOrderAnalytics: (query = {}) => call("GET", `/api/analytics/orders${analyticsQueryString(query)}`),
    getParkedNotices: () => call("GET", "/api/parked-notices"),
    getOperationsSummary: (incidentId) =>
      call("GET", `/api/operations/summary${queryString({ incident_id: incidentId })}`),
    getOperationsIncidents: (query = {}) =>
      call(
        "GET",
        `/api/operations/incidents${queryString({
          state: query.state,
          reason: query.reason,
          incident_id: query.incidentId,
          before: query.before,
        })}`,
      ),
    getOperationsLlm: () => call("GET", "/api/operations/llm"),
    getOperationsCarriers: () => call("GET", "/api/operations/carriers"),
    getOperationsFreshness: () => call("GET", "/api/operations/freshness"),
    getOperationsProfit: () => call("GET", "/api/operations/profit"),
    runCommand: (request) => call("POST", "/api/commands", request),
    runSupport: (request) => call("POST", "/api/support/runs", request),
    sendInput: async (runId, line) => {
      await call("POST", `/api/commands/${encodeURIComponent(runId)}/input`, { line });
    },
  };
}

function eventsQueryString(query: EventsQuery): string {
  const params = new URLSearchParams();
  if (query.limit !== undefined) params.set("limit", String(query.limit));
  if (query.beforeSeq !== undefined) params.set("before_seq", String(query.beforeSeq));
  if (query.incidentId) params.set("incident_id", query.incidentId);
  if (query.node) params.set("node", query.node);
  const text = params.toString();
  return text ? `?${text}` : "";
}

function analyticsQueryString(query: OrderAnalyticsQuery): string {
  const params = new URLSearchParams();
  if (query.status) params.set("status", query.status);
  if (query.incidentId) params.set("incident_id", query.incidentId);
  const text = params.toString();
  return text ? `?${text}` : "";
}

export function eventsStreamUrl(afterSeq?: number): string {
  return afterSeq === undefined ? "/api/events/stream" : `/api/events/stream?after_seq=${afterSeq}`;
}

// El detalle de un 422 de pydantic: una lista de errores con su `msg`.
function validationDetail(detail: unknown): string | null {
  if (!Array.isArray(detail)) return null;
  const messages = detail.map((item) => (typeof item?.msg === "string" ? item.msg : null)).filter(Boolean);
  return messages.length ? messages.join("; ") : null;
}

export function commandStreamUrl(runId: string): string {
  return `/api/commands/${encodeURIComponent(runId)}/stream`;
}

function queryString(values: Record<string, string | undefined>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) if (value) params.set(key, value);
  const text = params.toString();
  return text ? `?${text}` : "";
}
