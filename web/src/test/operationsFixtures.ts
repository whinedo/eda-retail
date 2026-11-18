// Datos de ejemplo de la vista de operaciones para los tests (spec operations-view).
import type { IncidentMetricsRow, Profit } from "../api/client";
import type { OperationsData } from "../useOperations";

export function incidentRow(incident_id: string, overrides: Partial<IncidentMetricsRow> = {}): IncidentMetricsRow {
  return {
    incident_id,
    carrier: "SEUR",
    cause: "loss",
    severity: "high",
    order_count: 1,
    state: "refunded",
    escalation_reason: null,
    plan_version: 2,
    steps_failed: 1,
    compensations: 0,
    opened_at: "2026-11-17T10:00:00+00:00",
    closed_at: "2026-11-17T10:00:12.5+00:00",
    duration_s: 12.5,
    tokens: 400,
    llm_cost: "0",
    effects_cost: "0",
    refunded: "450.00",
    last_event_at: "2026-11-17T10:00:12.5+00:00",
    last_event_type: "IncidentResolved",
    last_event_id: "d282ef26-1a2b-4c3d-9e8f-001122334455",
    kafka_partition: 1,
    kafka_offset: 130,
    ...overrides,
  };
}

export function operationsData(overrides: Partial<OperationsData> = {}): OperationsData {
  return {
    summary: {
      by_state: { open: 0, resolved: 23, refunded: 0, escalated: 7 },
      escalations: [{ reason: "llm_output_invalid", count: 7 }],
      by_plan_version: { "1": 40, "2": 10 },
      step_failures: [
        { action: "reserve_stock", error_code: "out_of_stock", count: 10 },
        { action: "file_claim", error_code: "timeout", count: 2 },
      ],
      cost_by_scenario: [
        { cause: "delay", severity: "low", incidents: 3, mean_tokens: 0, llm_cost: "0", effects_cost: "0", refunded: "0" },
        { cause: "loss", severity: "high", incidents: 2, mean_tokens: 500, llm_cost: "0", effects_cost: "0", refunded: "450" },
        { cause: null, severity: null, incidents: 1, mean_tokens: 50, llm_cost: "0", effects_cost: "0", refunded: "0" },
      ],
      prices_configured: false,
      applied: { "1": 129 },
    },
    incidents: [incidentRow("I-1")],
    nextBefore: null,
    llm: {
      agents: [
        { agent: "interpreter", calls: 0, tokens_in: 0, tokens_out: 0, latency_mean_ms: null, latency_p95_ms: null, validated_pct: null },
        { agent: "planner", calls: 20, tokens_in: 2000, tokens_out: 400, latency_mean_ms: 1050, latency_p95_ms: 1900, validated_pct: 75 },
      ],
      applied: {},
    },
    carriers: { carriers: [{ carrier: "SEUR", incidents: 3, delays: 1, claims_filed: 1, refunds_issued: 1 }], consults: [], applied: {} },
    freshness: {
      consumers: [
        { consumer: "event_log", partitions: [{ partition: 1, applied: 129, end_offset: 130, lag: 0 }], lag: 0 },
        { consumer: "operations", partitions: [{ partition: 1, applied: 126, end_offset: 130, lag: 3 }], lag: 3 },
      ],
    },
    ...overrides,
  };
}

// Beneficio de la cascada de la spec operations-view: 300 de ingresos, 30 + 20 de transporte y 100 de reembolsos.
export function profit(overrides: Partial<Profit> = {}): Profit {
  return {
    revenue: "300.00",
    shipping: "30.00",
    reissue_shipping: "20.00",
    reissue_fixed: "0.00",
    refunds: "100.00",
    claims: "0.00",
    llm: "0.00",
    profit: "150.00",
    shipping_pct: "10",
    applied: { "0": 12 },
    ...overrides,
  };
}
