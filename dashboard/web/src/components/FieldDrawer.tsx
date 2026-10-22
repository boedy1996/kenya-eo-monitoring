import ReactECharts from "echarts-for-react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api";
import type { Filters } from "../types";

type Props = { fieldId: string; filters: Filters; onClose: () => void };

const percentage = (value: number | null | undefined) =>
  value == null ? "—" : `${Math.round(value * 100)}%`;

const readable = (value: string | null | undefined) =>
  value ? value.replaceAll("_", " ") : "—";

const decimal = (value: number | null | undefined, digits = 2) =>
  value == null ? "—" : value.toLocaleString(undefined, { maximumFractionDigits: digits });

export function FieldDrawer({ fieldId, filters, onClose }: Props) {
  const isSeasonal = filters.mode === "seasonal";
  const weeklyDetail = useQuery({
    queryKey: ["field", fieldId, filters.week],
    queryFn: () => api.field(fieldId, filters.week),
    enabled: !isSeasonal,
  });
  const seasonalDetail = useQuery({
    queryKey: ["seasonal-field", fieldId, filters.season],
    queryFn: () => api.seasonalField(fieldId, filters.season),
    enabled: isSeasonal,
  });
  const seasonalField = seasonalDetail.data?.field;
  const dateFrom = isSeasonal ? seasonalField?.season_start : undefined;
  const dateTo = isSeasonal ? seasonalField?.season_end : undefined;
  const history = useQuery({
    queryKey: ["predictions", fieldId, dateFrom, dateTo],
    queryFn: () => api.predictions(fieldId, dateFrom, dateTo),
    enabled: !isSeasonal || Boolean(dateFrom && dateTo),
  });
  const signals = useQuery({
    queryKey: ["signals", fieldId, dateFrom, dateTo],
    queryFn: () => api.signals(fieldId, dateFrom, dateTo),
    enabled: !isSeasonal || Boolean(dateFrom && dateTo),
  });

  const weeklyField = weeklyDetail.data?.field;
  const field = isSeasonal ? seasonalField : weeklyField;
  const detailLoading = isSeasonal ? seasonalDetail.isLoading : weeklyDetail.isLoading;
  const detailError = isSeasonal ? seasonalDetail.error : weeklyDetail.error;
  const chartOption = {
    animation: false,
    tooltip: { trigger: "axis" },
    legend: { top: 0, textStyle: { color: "#526158", fontSize: 10 } },
    grid: { left: 38, right: 12, top: 38, bottom: 28 },
    xAxis: {
      type: "category",
      data: signals.data?.s2.map((row) => row.start_date) ?? [],
      axisLabel: { hideOverlap: true, color: "#738078" },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#738078" },
      splitLine: { lineStyle: { color: "#e4e8e1" } },
    },
    series: [
      {
        name: "NDVI",
        type: "line",
        symbol: "none",
        data: signals.data?.s2.map((row) => row.ndvi_mean),
        lineStyle: { color: "#3f8f59" },
      },
      {
        name: "EVI",
        type: "line",
        symbol: "none",
        data: signals.data?.s2.map((row) => row.evi_mean),
        lineStyle: { color: "#c58537" },
      },
      {
        name: "LAI",
        type: "line",
        symbol: "none",
        data: signals.data?.s2.map((row) => row.lai_mean),
        lineStyle: { color: "#367fba" },
      },
    ],
  };

  const selectedSignal = signals.data?.s2.find((row) => row.start_date === filters.week);
  const historyRows = history.data?.predictions ?? [];
  const visibleHistory = isSeasonal ? historyRows : historyRows.slice(-8);

  return (
    <aside className="field-drawer" aria-live="polite">
      <header>
        <div>
          <span className="eyebrow">Field observation</span>
          <h2>{fieldId}</h2>
        </div>
        <button onClick={onClose} aria-label="Close field details">×</button>
      </header>
      {detailLoading && <p className="muted">Loading field…</p>}
      {detailError && <p className="error">Unable to load this field.</p>}
      {field && (
        <>
          <div className="field-meta">
            <span>{field.county}</span>
            <span>{field.area_ha == null ? "Area unavailable" : `${field.area_ha.toFixed(2)} ha`}</span>
            <span>
              {isSeasonal
                ? `${seasonalField?.season_start} – ${seasonalField?.season_end}`
                : filters.week}
            </span>
          </div>

          {isSeasonal && seasonalField ? (
            <>
              <div className={`prediction-card label-${seasonalField.seasonal_label.toLowerCase()}`}>
                <span>Seasonal call</span>
                <strong>{seasonalField.seasonal_label}</strong>
                <div><b>{percentage(seasonalField.seasonal_confidence)}</b> confidence</div>
                <small>{readable(seasonalField.status)}</small>
              </div>
              <section className="quality-card">
                <h3>Seasonal evidence</h3>
                <dl>
                  <div><dt>Leading crop</dt><dd>{readable(seasonalField.leading_label)}</dd></div>
                  <div><dt>Classified weeks</dt><dd>{seasonalField.classified_weeks} / {seasonalField.total_weeks}</dd></div>
                  <div><dt>Leading appearances</dt><dd>{seasonalField.appearance_count ?? "—"}</dd></div>
                  <div><dt>Vote share</dt><dd>{percentage(seasonalField.vote_share)}</dd></div>
                  <div><dt>Median confidence</dt><dd>{percentage(seasonalField.median_confidence)}</dd></div>
                  <div><dt>Maximum confidence</dt><dd>{percentage(seasonalField.maximum_confidence)}</dd></div>
                  <div><dt>Runner-up crop</dt><dd>{readable(seasonalField.runner_up_label)}</dd></div>
                  <div><dt>Runner-up strength</dt><dd>{percentage(seasonalField.runner_up_confidence)}</dd></div>
                </dl>
              </section>
              <section className="quality-card yield-detail-card">
                <div className="section-title">
                  <h3>Calculated yield</h3>
                  <span>
                    {seasonalField.yield_run_id
                      ? readable(seasonalField.yield_status)
                      : "Unavailable"}
                  </span>
                </div>
                {seasonalField.yield_central_t_ha != null ? (
                  <>
                    <div className="yield-scenario">
                      <div><span>Low</span><strong>{decimal(seasonalField.yield_low_t_ha)} t/ha</strong></div>
                      <div className="central"><span>Central</span><strong>{decimal(seasonalField.yield_central_t_ha)} t/ha</strong></div>
                      <div><span>High</span><strong>{decimal(seasonalField.yield_high_t_ha)} t/ha</strong></div>
                    </div>
                    <dl>
                      <div><dt>Central production</dt><dd>{decimal(seasonalField.production_central_t, 1)} t</dd></div>
                      <div><dt>Seasonal NPP</dt><dd>{decimal(seasonalField.seasonal_npp_gC_m2, 1)} gC/m²</dd></div>
                      <div><dt>NPP temporal coverage</dt><dd>{percentage(seasonalField.npp_temporal_coverage)}</dd></div>
                      <div><dt>NPP spatial coverage</dt><dd>{percentage(seasonalField.npp_mean_spatial_coverage)}</dd></div>
                      <div><dt>Effective WaPOR pixels</dt><dd>{decimal(seasonalField.npp_mean_effective_pixels, 1)}</dd></div>
                      <div><dt>Phenology</dt><dd>{seasonalField.sos} – {seasonalField.eos}</dd></div>
                      <div><dt>Phenology method</dt><dd>{readable(seasonalField.phenology_method)}</dd></div>
                      <div><dt>Intercrop status</dt><dd>{readable(seasonalField.intercrop_status)}</dd></div>
                    </dl>
                  </>
                ) : seasonalField.yield_run_id ? (
                  <div className="yield-unavailable">
                    <strong>Yield was not calculated for this field.</strong>
                    <span>{readable(seasonalField.yield_qa_flags)}</span>
                  </div>
                ) : (
                  <div className="yield-unavailable">
                    <strong>No validated yield result for this season.</strong>
                  </div>
                )}
                <p className="yield-caveat">Unvalidated remote sensing estimate; no crop cut calibration.</p>
              </section>
            </>
          ) : weeklyField ? (
            <>
              <div className={`prediction-card label-${weeklyField.label.toLowerCase()}`}>
                <span>Model call</span>
                <strong>{weeklyField.label}</strong>
                <div><b>{percentage(weeklyField.confidence)}</b> confidence</div>
                <small>{readable(weeklyField.reason)}</small>
              </div>
              <div className="probability-grid">
                <div><span>Maize</span><strong>{percentage(weeklyField.p_maize)}</strong></div>
                <div><span>Beans</span><strong>{percentage(weeklyField.p_beans)}</strong></div>
                <div><span>Potatoes</span><strong>{percentage(weeklyField.p_potatoes)}</strong></div>
              </div>
              <section className="quality-card">
                <h3>Pixel audit</h3>
                <dl>
                  <div><dt>Buffer used</dt><dd>{selectedSignal?.buffer_width_m ?? "—"} m</dd></div>
                  <div><dt>Before buffer</dt><dd>{selectedSignal?.pixel_count_before ?? "—"}</dd></div>
                  <div><dt>After buffer</dt><dd>{selectedSignal?.pixel_count_after ?? "—"}</dd></div>
                </dl>
              </section>
            </>
          ) : null}

          <section className="chart-card">
            <div className="section-title">
              <h3>Vegetation signals</h3>
              <span>{isSeasonal ? "Selected season" : "Weekly means"}</span>
            </div>
            {signals.isLoading ? (
              <p className="muted">Loading signals…</p>
            ) : (
              <ReactECharts option={chartOption} style={{ height: 230 }} />
            )}
          </section>
          <section className="history-card">
            <div className="section-title">
              <h3>Prediction history</h3>
              <span>{historyRows.length} weeks</span>
            </div>
            <div className="history-list">
              {[...visibleHistory].reverse().map((item) => (
                <div key={item.week_start}>
                  <time>{item.week_start}</time>
                  <span className={`history-label label-${item.label.toLowerCase()}`}>{item.label}</span>
                  <strong>{percentage(item.confidence)}</strong>
                </div>
              ))}
            </div>
          </section>
        </>
      )}
    </aside>
  );
}
