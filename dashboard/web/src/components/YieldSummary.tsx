import type { SeasonalYieldSummary } from "../types";

const format = (value: number | null, digits = 2) =>
  value == null ? "—" : value.toLocaleString(undefined, { maximumFractionDigits: digits });

export function YieldSummary({ data }: { data: SeasonalYieldSummary | undefined }) {
  if (!data?.yield_run) {
    return (
      <section className="yield-summary unavailable">
        <div className="section-title"><h3>Calculated yield</h3><span>Unavailable</span></div>
        <p>No validated yield run is available for this season.</p>
      </section>
    );
  }
  const aggregates = data.aggregates ?? [];
  if (!aggregates.length) {
    return (
      <section className="yield-summary unavailable">
        <div className="section-title"><h3>Calculated yield</h3><span>Unavailable</span></div>
        <p>The versioned area-weighted aggregation has not been built for this season.</p>
      </section>
    );
  }
  const fields = aggregates.reduce((sum, group) => sum + group.calculated_fields, 0);
  const production = aggregates.reduce(
    (sum, group) => sum + (group.production_central_t ?? 0), 0,
  );
  const crops = [...new Set(aggregates.map((group) => group.crop_label))];
  return (
    <section className="yield-summary">
      <div className="section-title">
        <h3>Calculated yield</h3>
        <span>Unvalidated</span>
      </div>
      <div className="yield-totals">
        <div><span>Calculated fields</span><strong>{fields.toLocaleString()}</strong></div>
        <div><span>Central production</span><strong>{format(production, 0)} t</strong></div>
      </div>
      <div className="yield-crop-list">
        {crops.map((crop) => {
          const rows = aggregates.filter((group) => group.crop_label === crop);
          const cropFields = rows.reduce((sum, group) => sum + group.calculated_fields, 0);
          const mappedArea = rows.reduce((sum, group) => sum + group.mapped_area_ha, 0);
          const cropProduction = rows.reduce(
            (sum, group) => sum + (group.production_central_t ?? 0), 0,
          );
          const areaWeightedYield = mappedArea > 0
            ? cropProduction / mappedArea
            : null;
          return (
            <div key={crop}>
              <span>{crop}</span>
              <strong>{format(areaWeightedYield)} t/ha</strong>
              <small>{cropFields.toLocaleString()} fields · area-weighted</small>
            </div>
          );
        })}
      </div>
      <p className="yield-caveat">
        Remote sensing calculation without observed yield calibration.
        {data.aggregation ? ` Aggregation: ${data.aggregation.method_version}.` : ""}
      </p>
    </section>
  );
}
