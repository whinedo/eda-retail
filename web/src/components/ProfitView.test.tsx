import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { profit } from "../test/operationsFixtures";
import { ProfitView, type ProfitViewProps } from "./ProfitView";

function setup(props: Partial<ProfitViewProps> = {}) {
  const handlers = { onOperations: vi.fn(), onBack: vi.fn() };
  render(<ProfitView data={profit()} error={null} {...handlers} {...props} />);
  return { ...handlers, user: userEvent.setup() };
}

const view = () => screen.getByRole("region", { name: "Beneficio" });
const chart = () => within(view()).getByRole("img", { name: /Cascada del beneficio/ });

describe("Pantalla de beneficio", () => {
  it("Cascada con un reenvío y un reembolso: barra de ingresos, una por gasto y la del beneficio, con 150,00 € de total", () => {
    setup();
    expect(within(view()).getByText("150,00 €", { selector: ".profit__total strong" })).toBeInTheDocument();
    const bars = chart().querySelectorAll("[data-bar]");
    expect([...bars].map((b) => b.getAttribute("data-bar"))).toEqual([
      "revenue",
      "shipping",
      "reissue_shipping",
      "reissue_fixed",
      "refunds",
      "claims",
      "llm",
      "profit",
    ]);
    const table = within(view()).getByRole("table", { name: "Líneas del beneficio" });
    const cells = (name: string) => within(within(table).getByRole("row", { name: new RegExp(name) })).getAllByRole("cell");
    expect(cells("Ingresos").map((c) => c.textContent)).toEqual(["Ingresos", "300,00 €"]);
    expect(cells("Transporte de los reenvíos").map((c) => c.textContent)).toEqual(["Transporte de los reenvíos", "−20,00 €"]);
    expect(cells("Beneficio total").map((c) => c.textContent)).toEqual(["Beneficio total", "150,00 €"]);
  });

  it("Pantalla de beneficio: indica el porcentaje de transporte y que no cuenta el artículo reenviado", () => {
    setup();
    expect(within(view()).getByText(/10 % del importe de cada pedido/)).toBeInTheDocument();
    expect(within(view()).getByText(/coste del artículo reenviado no se cuenta/)).toBeInTheDocument();
  });

  it("Beneficio positivo: su barra va en verde", () => {
    setup();
    expect(chart().querySelector('[data-bar="profit"]')).toHaveClass("profit__bar--gain");
  });

  it("Beneficio negativo: el total aparece en negativo y su barra en rojo", () => {
    setup({ data: profit({ refunds: "400.00", profit: "-150.00" }) });
    expect(within(view()).getByText("−150,00 €", { selector: ".profit__total strong" })).toBeInTheDocument();
    expect(chart().querySelector('[data-bar="profit"]')).toHaveClass("profit__bar--loss");
  });

  it("De operaciones al beneficio y vuelta: «← Operaciones» y «← Vista principal» llaman a sus manejadores", async () => {
    const { user, onOperations, onBack } = setup();
    await user.click(screen.getByRole("link", { name: "← Operaciones" }));
    await user.click(screen.getByRole("link", { name: "← Vista principal" }));
    expect(onOperations).toHaveBeenCalledOnce();
    expect(onBack).toHaveBeenCalledOnce();
  });

  it("Fallo de carga: muestra el error en rojo", () => {
    setup({ data: null, error: "HTTP 500" });
    expect(within(view()).getByText("HTTP 500")).toHaveClass("events__error");
  });

  it("Sin datos todavía: no dibuja la gráfica", () => {
    setup({ data: null });
    expect(within(view()).queryByRole("img")).not.toBeInTheDocument();
  });
});
