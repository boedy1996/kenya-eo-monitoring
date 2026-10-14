import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "./api";
import { FilterBar } from "./components/FilterBar";
import { KpiStrip } from "./components/KpiStrip";
import { MapPanel } from "./components/MapPanel";
import type { Filters } from "./types";

const initialFilters: Filters = { week: "", county: "", label: "", reason: "", minConfidence: 0 };
const FieldDrawer = lazy(() =>
  import("./components/FieldDrawer").then((module) => ({ default: module.FieldDrawer })),
);

export default function App() {
  const [filters, setFilters] = useState(initialFilters);
  const [selectedField, setSelectedField] = useState("");
  const [search, setSearch] = useState("");
  const catalog = useQuery({ queryKey: ["catalog"], queryFn: api.catalog });
  const weeks = useQuery({ queryKey: ["weeks"], queryFn: api.weeks });

  useEffect(() => {
    if (!filters.week && weeks.data?.weeks.length) {
      setFilters((current) => ({ ...current, week: weeks.data!.weeks.at(-1)!.week_start }));
    }
  }, [filters.week, weeks.data]);

  const summary = useQuery({
    queryKey: ["summary", filters],
    queryFn: () => api.summary(filters),
    enabled: Boolean(filters.week),
  });
  const searchResults = useQuery({
    queryKey: ["field-search", search],
    queryFn: () => api.search(search),
    enabled: search.length >= 2,
  });
  const totalFields = useMemo(
    () => summary.data?.groups.reduce((total, group) => total + group.field_count, 0) ?? 0,
    [summary.data],
  );

  if (catalog.isLoading || weeks.isLoading) {
    return <main className="loading-screen"><div className="pulse" /><p>Preparing crop monitor…</p></main>;
  }
  if (catalog.error || weeks.error || !catalog.data) {
    return (
      <main className="loading-screen error-screen">
        <h1>Dashboard data is unavailable</h1>
        <p>Start the FastAPI service and load the development bundle, then refresh this page.</p>
      </main>
    );
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand-mark">KE</div>
        <div className="brand-copy">
          <span>Earth observation</span>
          <h1>Kenya Crop Monitor</h1>
        </div>
        <div className="pilot-banner"><span>●</span> Pilot baseline</div>
        <div className="run-context">
          <span>Model run</span>
          <strong>{catalog.data.model_run.model_run_id}</strong>
        </div>
      </header>

      <FilterBar catalog={catalog.data} weeks={weeks.data?.weeks ?? []} filters={filters} onChange={setFilters} />

      <main className="workspace">
        <section className="map-column">
          <div className="map-toolbar">
            <div>
              <span className="eyebrow">Current selection</span>
              <strong>{totalFields.toLocaleString()} field records</strong>
            </div>
            <div className="field-search">
              <input
                value={search}
                placeholder="Find field ID"
                onChange={(event) => setSearch(event.target.value)}
              />
              {searchResults.data && search.length >= 2 && (
                <div className="search-results">
                  {searchResults.data.fields.map((field) => (
                    <button key={field.field_id} onClick={() => { setSelectedField(field.field_id); setSearch(""); }}>
                      <strong>{field.field_id}</strong><span>{field.county}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          </div>
          <MapPanel filters={filters} onSelectField={setSelectedField} />
          <div className="legend">
            <span><i className="maize" />Maize</span>
            <span><i className="beans" />Beans</span>
            <span><i className="potatoes" />Potatoes</span>
            <span><i className="unclassified" />Unclassified</span>
          </div>
        </section>
        <aside className="insight-column">
          <div className="section-title"><div><span className="eyebrow">Weekly snapshot</span><h2>Prediction overview</h2></div><span>{filters.week}</span></div>
          <KpiStrip groups={summary.data?.groups ?? []} />
          <section className="model-note">
            <h3>How to read this map</h3>
            <p>Every field is assigned a model label or marked Unclassified with a reason. Confidence describes model certainty within the pilot classes.</p>
          </section>
          <section className="coverage-card">
            <div><span>Fields in model run</span><strong>{catalog.data.model_run.field_count.toLocaleString()}</strong></div>
            <div><span>Available weeks</span><strong>{catalog.data.model_run.week_count}</strong></div>
            <div><span>Prediction rows</span><strong>{catalog.data.model_run.prediction_rows.toLocaleString()}</strong></div>
          </section>
        </aside>
      </main>
      {selectedField && filters.week && (
        <Suspense fallback={<div className="drawer-loading">Loading field details…</div>}>
          <FieldDrawer fieldId={selectedField} week={filters.week} onClose={() => setSelectedField("")} />
        </Suspense>
      )}
    </div>
  );
}
