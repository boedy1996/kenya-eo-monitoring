import type { Catalog, Filters, SeasonDefinition, Week } from "../types";

type Props = {
  catalog: Catalog;
  weeks: Week[];
  seasons: SeasonDefinition[];
  filters: Filters;
  onChange: (filters: Filters) => void;
};

export function FilterBar({ catalog, weeks, seasons, filters, onChange }: Props) {
  const set = <K extends keyof Filters>(key: K, value: Filters[K]) =>
    onChange({ ...filters, [key]: value });
  const selectedWeek = weeks.find((item) => item.week_start === filters.week);
  const selectedSeason = seasons.find((item) => item.season_code === filters.season);
  const reasonOptions = filters.mode === "seasonal"
    ? ["final", "provisional", "ambiguous", "insufficient_data"]
    : catalog.reasons;

  return (
    <section className="filter-bar" aria-label="Map filters">
      <label>
        <span>View</span>
        <select
          value={filters.mode}
          onChange={(event) => onChange({
            ...filters,
            mode: event.target.value as Filters["mode"],
            reason: "",
          })}
        >
          <option value="weekly">Weekly</option>
          <option value="seasonal">Seasonal</option>
        </select>
      </label>
      <label>
        <span>{filters.mode === "seasonal" ? "Season" : "Week"}</span>
        <select
          value={filters.mode === "seasonal" ? filters.season : filters.week}
          onChange={(event) => {
            if (filters.mode === "seasonal") set("season", event.target.value);
            else set("week", event.target.value);
          }}
        >
          {filters.mode === "seasonal"
            ? seasons.map((season) => (
                <option value={season.season_code} key={season.season_code}>
                  {season.season_year} {season.season_name}
                </option>
              ))
            : weeks.map((week) => (
                <option value={week.week_start} key={week.week_start}>
                  {week.week_start}
                </option>
              ))}
        </select>
      </label>
      <div className="season-context">
        <span>
          {filters.mode === "seasonal"
            ? `${selectedSeason?.season_start ?? "—"} – ${selectedSeason?.season_end ?? "—"}`
            : selectedWeek?.season ?? "Inter-season"}
        </span>
        <strong>
          {filters.mode === "seasonal"
            ? selectedSeason?.yield_available
              ? "Final · Yield available"
              : selectedSeason?.is_complete ? "Final · Yield pending" : "Provisional"
            : selectedWeek?.week_in_season ? `Horizon ${selectedWeek.week_in_season}` : "No horizon"}
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
        <span>{filters.mode === "seasonal" ? "Status" : "Reason"}</span>
        <select value={filters.reason} onChange={(event) => set("reason", event.target.value)}>
          <option value="">
            {filters.mode === "seasonal" ? "All statuses" : "All reasons"}
          </option>
          {reasonOptions.map((reason) => (
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
