import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { ParkedNotice } from "../api/client";
import { ParkedNoticesBanner, formatDeadline } from "./ParkedNoticesBanner";

function notice(incident_id: string, order_ids: string[], deadline_at: string): ParkedNotice {
  return {
    incident_id,
    order_ids,
    deadline_at,
    parked_at: "2026-11-15T20:08:36+00:00",
    last_event_id: "d282ef26-1a2b-4c3d-9e8f-001122334455",
    kafka_partition: 0,
    kafka_offset: 3,
  };
}

describe("ParkedNoticesBanner", () => {
  it("lista cada aviso con su incidencia corta, sus pedidos y su plazo en hora local", () => {
    const first = notice("I-3f2a9c1b-0000-5000-8000-000000000000", ["P-1001"], "2026-11-15T20:13:36+00:00");
    const second = notice("I-77aa00bb-0000-5000-8000-000000000000", ["P-1004", "P-1005"], "2026-11-15T20:14:02+00:00");

    render(<ParkedNoticesBanner notices={[first, second]} />);

    const banner = screen.getByRole("status", { name: "Avisos aparcados" });
    expect(banner).toHaveTextContent("⚠ 2 avisos aparcados esperan pedidos que aún no existen:");
    expect(banner).toHaveTextContent(`P-1001 (I-3f2a9c1b, vence ${formatDeadline(first.deadline_at)})`);
    expect(banner).toHaveTextContent(`P-1004, P-1005 (I-77aa00bb, vence ${formatDeadline(second.deadline_at)})`);
    expect(banner).toHaveTextContent("Si no has sembrado los pedidos, usa «seed».");
    expect(screen.getByText("I-3f2a9c1b")).toHaveAttribute("title", first.incident_id);
  });

  it("en singular con un solo aviso", () => {
    render(<ParkedNoticesBanner notices={[notice("I-1", ["P-1"], "2026-11-15T20:13:36+00:00")]} />);
    expect(screen.getByRole("status")).toHaveTextContent("⚠ 1 aviso aparcado espera pedidos que aún no existen:");
  });

  it("no se muestra sin avisos aparcados", () => {
    const { container } = render(<ParkedNoticesBanner notices={[]} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("el plazo va en hora local (HH:MM:SS)", () => {
    const iso = "2026-11-15T20:13:36+00:00";
    const local = new Date(iso);
    const pad = (n: number) => String(n).padStart(2, "0");
    expect(formatDeadline(iso)).toBe(`${pad(local.getHours())}:${pad(local.getMinutes())}:${pad(local.getSeconds())}`);
  });
});
