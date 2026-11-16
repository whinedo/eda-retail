import { describe, expect, it } from "vitest";
import { layoutNodes, POSITIONS } from "./mapLayout";

describe("layoutNodes", () => {
  it("gives the A2A carrier agent its own box, tall and wide enough for a two-line label", () => {
    const boxes = layoutNodes(["event_log", "carrier_agent"]);
    const box = boxes.get("carrier_agent");
    expect(box).toEqual(POSITIONS["carrier_agent"]);
    expect(box!.w).toBeGreaterThanOrEqual(170);
    expect(box!.h).toBeGreaterThanOrEqual(52);
  });

  it("keeps placing unknown nodes in the fallback area", () => {
    const boxes = layoutNodes(["unknown_a", "unknown_b"]);
    expect(boxes.get("unknown_a")).toEqual({ x: 190, y: 440, w: 130, h: 44 });
    expect(boxes.get("unknown_b")).toEqual({ x: 334, y: 440, w: 130, h: 44 });
  });
});
