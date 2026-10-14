import type { Catalog, Filters, Week } from "../types";

type Props = {
  catalog: Catalog;
  weeks: Week[];
  filters: Filters;
  onChange: (filters: Filters) => void;
};

export function FilterBar({ catalog, weeks, filters, onChange }: Props) {
  const set = <K extends keyof Filters>(key: K, value: Filters[K]) =>
    onChange({ ...filters, [key]: value });
  const selectedWeek = weeks.find((item) => item.week_start === filters.week);

  return (
    <section className="filter-bar" aria-label="Map filters">
      <label>
        <span>Week</span>
        <select value={filters.week} onChange={(event) => set("week", event.target.value)}>
          {weeks.map((week) => (
            <option value={week.week_start} key={week.week_start}>
              {week.week_start}
            </option>
          ))}
        </select>
      </label>
      <div className="season-context">
        <span>{selectedWeek?.season ?? "Inter-season"}</span>
        <strong>
          {selectedWeek?.week_in_season ? `Horizon ${selectedWeek.week_in_season}` : "No horizon"}
        </strong>
      </div>
      <label>
        <span>County</span>
        <select value={filters.county} onChange={(event) => set("county", event.target.value)}>
          <option value="">All counties</option>
          {catalog.counties.map((county) => (
            <option key={county}>{county}</option>
          ))}
        </select>
      </label>
      <label>
        <span>Prediction</span>
        <select value={filters.label} onChange={(event) => set("label", event.target.value)}>
          <option value="">All labels</option>
          {catalog.labels.map((label) => (
            <option key={label}>{label}</option>
          ))}
        </select>
      </label>
      <label>
        <span>Reason</span>
        <select value={filters.reason} onChange={(event) => set("reason", event.target.value)}>
          <option value="">All reasons</option>
          {catalog.reasons.map((reason) => (
            <option key={reason}>{reason.replaceAll("_", " ")}</option>
          ))}
        </select>
      </label>
      <label className="confidence-filter">
        <span>Minimum confidence</span>
        <strong>{Math.round(filters.minConfidence * 100)}%</strong>
        <input
          type="range"
          min="0"
          max="1"
          step="0.05"
          value={filters.minConfidence}
          onChange={(event) => set("minConfidence", Number(event.target.value))}
        />
      </label>
    </section>
  );
}
