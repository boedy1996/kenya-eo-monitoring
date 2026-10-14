import type { SummaryGroup } from "../types";

const order = ["maize", "beans", "potatoes", "Unclassified"];

export function KpiStrip({ groups }: { groups: SummaryGroup[] }) {
  const counts = new Map<string, number>();
  for (const group of groups) counts.set(group.label, (counts.get(group.label) ?? 0) + group.field_count);
  return (
    <section className="kpi-strip" aria-label="Prediction totals">
      {order.map((label) => (
        <article className={`kpi kpi-${label.toLowerCase()}`} key={label}>
          <span>{label}</span>
          <strong>{(counts.get(label) ?? 0).toLocaleString()}</strong>
          <small>fields</small>
        </article>
      ))}
    </section>
  );
}
