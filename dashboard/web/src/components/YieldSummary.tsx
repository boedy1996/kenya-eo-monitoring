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
  const calculated = data.groups.filter((group) => group.calculated_fields > 0);
  const fields = calculated.reduce((sum, group) => sum + group.calculated_fields, 0);
  const production = calculated.reduce(
    (sum, group) => sum + (group.production_central_t ?? 0), 0,
  );
  const crops = [...new Set(calculated.map((group) => group.crop_label))];
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
          const rows = calculated.filter((group) => group.crop_label === crop);
          const cropFields = rows.reduce((sum, group) => sum + group.calculated_fields, 0);
          const weightedMean = cropFields
            ? rows.reduce(
                (sum, group) => sum + (group.yield_central_mean ?? 0) * group.calculated_fields,
                0,
              ) / cropFields
            : null;
          return (
            <div key={crop}>
              <span>{crop}</span>
              <strong>{format(weightedMean)} t/ha</strong>
              <small>{cropFields.toLocaleString()} fields</small>
            </div>
          );
        })}
      </div>
      <p className="yield-caveat">Remote sensing calculation without observed yield calibration.</p>
    </section>
  );
}
