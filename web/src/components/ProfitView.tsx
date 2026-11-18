import type { Profit } from "../api/client";

export interface ProfitViewProps {
  data: Profit | null;
  error: string | null;
  onOperations(): void;
  onBack(): void;
}

type CostKey = "shipping" | "reissue_shipping" | "reissue_fixed" | "refunds" | "claims" | "llm";

// Gastos en el orden de la cascada (spec operations-view, «Pantalla de beneficio»).
const COSTS: { key: CostKey; label: string; short: string }[] = [
  { key: "shipping", label: "Transporte", short: "Transporte" },
  { key: "reissue_shipping", label: "Transporte de los reenvíos", short: "Transp. reenvíos" },
  { key: "reissue_fixed", label: "Coste fijo de los reenvíos", short: "Reenvíos" },
  { key: "refunds", label: "Reembolsos", short: "Reembolsos" },
  { key: "claims", label: "Reclamaciones", short: "Reclamaciones" },
  { key: "llm", label: "LLM", short: "LLM" },
];

const MINUS = "−";

export function euros(value: number): string {
  const text = Math.abs(value).toLocaleString("es-ES", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return `${value < 0 ? MINUS : ""}${text} €`;
}

interface Bar {
  key: string;
  label: string;
  from: number;
  to: number;
  kind: "revenue" | "cost" | "gain" | "loss";
}

function bars(data: Profit): Bar[] {
  const revenue = Number(data.revenue);
  const result: Bar[] = [{ key: "revenue", label: "Ingresos", from: 0, to: revenue, kind: "revenue" }];
  let level = revenue;
  for (const { key, short } of COSTS) {
    const cost = Number(data[key]);
    result.push({ key, label: short, from: level, to: level - cost, kind: "cost" });
    level -= cost;
  }
  const profit = Number(data.profit);
  result.push({ key: "profit", label: "Beneficio", from: 0, to: profit, kind: profit < 0 ? "loss" : "gain" });
  return result;
}

const WIDTH = 760;
const HEIGHT = 280;
const TOP = 24;
const BOTTOM = 40;
const SLOT = WIDTH / 8;

function Waterfall({ data }: { data: Profit }) {
  const items = bars(data);
  const values = items.flatMap((b) => [b.from, b.to]);
  const max = Math.max(0, ...values);
  const min = Math.min(0, ...values);
  const span = max - min || 1;
  const y = (v: number) => TOP + ((max - v) / span) * (HEIGHT - TOP - BOTTOM);
  return (
    <svg
      className="profit__chart"
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      role="img"
      aria-label={`Cascada del beneficio: ingresos ${euros(Number(data.revenue))}, beneficio ${euros(Number(data.profit))}`}
    >
      <line className="profit__axis" x1={0} x2={WIDTH} y1={y(0)} y2={y(0)} />
      {items.map((b, i) => {
        const top = Math.min(y(b.from), y(b.to));
        const height = Math.max(1, Math.abs(y(b.from) - y(b.to)));
        const x = i * SLOT + SLOT * 0.15;
        const amount = b.kind === "cost" ? b.to - b.from : b.to;
        return (
          <g key={b.key}>
            <rect data-bar={b.key} className={`profit__bar profit__bar--${b.kind}`} x={x} y={top} width={SLOT * 0.7} height={height} />
            <text className="profit__value" x={x + SLOT * 0.35} y={top - 6} textAnchor="middle">
              {euros(amount)}
            </text>
            <text className="profit__label" x={x + SLOT * 0.35} y={HEIGHT - BOTTOM + 18} textAnchor="middle">
              {b.label}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

function link(label: string, onClick: () => void, href: string) {
  return (
    <a
      className="analytics__back"
      href={href}
      onClick={(e) => {
        e.preventDefault();
        onClick();
      }}
    >
      {label}
    </a>
  );
}

// Pantalla de beneficio: presentacional; la consulta vive en useProfit.
export function ProfitView({ data, error, onOperations, onBack }: ProfitViewProps) {
  const profit = data ? Number(data.profit) : 0;
  return (
    <section className="analytics profit" aria-labelledby="profit-title">
      <div className="analytics__top">
        {link("← Vista principal", onBack, "#/")}
        {link("← Operaciones", onOperations, "#/analytics/operations")}
        <h2 id="profit-title" className="panel-title">
          Beneficio
        </h2>
      </div>
      {error && <p className="events__error">{error}</p>}
      {data && (
        <div className="profit__body">
          <p className="profit__total">
            Beneficio total <strong className={profit < 0 ? "profit__loss" : "profit__gain"}>{euros(profit)}</strong>
          </p>
          <Waterfall data={data} />
          <table className="events__table profit__table" aria-label="Líneas del beneficio">
            <tbody>
              <tr>
                <td>Ingresos</td>
                <td>{euros(Number(data.revenue))}</td>
              </tr>
              {COSTS.map(({ key, label }) => (
                <tr key={key}>
                  <td>{label}</td>
                  <td>{euros(-Number(data[key]))}</td>
                </tr>
              ))}
              <tr className="operations__total">
                <td>Beneficio total</td>
                <td>{euros(profit)}</td>
              </tr>
            </tbody>
          </table>
          <p className="operations__note">
            Ingresos: el importe de todos los pedidos registrados. Transporte: el {Number(data.shipping_pct).toLocaleString("es-ES")} % del
            importe de cada pedido (SHIPPING_COST_PCT), y otra vez en cada reenvío. El coste del artículo reenviado no se cuenta.
          </p>
        </div>
      )}
    </section>
  );
}
