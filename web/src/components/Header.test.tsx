import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Header } from "./Header";

const LOADING = { kind: "loading" } as const;

describe("Insignia de la versión de la PoC", () => {
  it("versión 1: muestra «Versión 1»", () => {
    render(<Header health={LOADING} jaegerUrl="" pocVersion={1} />);
    expect(screen.getByText("Versión 1")).toBeInTheDocument();
    expect(screen.queryByText(/MCP \+ A2A/)).not.toBeInTheDocument();
  });

  it("versión 2: muestra «Versión 2 (MCP + A2A)»", () => {
    render(<Header health={LOADING} jaegerUrl="" pocVersion={2} />);
    expect(screen.getByText("Versión 2 (MCP + A2A)")).toBeInTheDocument();
  });

  it("mientras no se conoce la versión, no muestra la insignia", () => {
    render(<Header health={LOADING} jaegerUrl="" />);
    expect(screen.queryByText(/^Versión/)).not.toBeInTheDocument();
  });
});
