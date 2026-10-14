import ReactECharts from "echarts-for-react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api";

type Props = { fieldId: string; week: string; onClose: () => void };

const percentage = (value: number | null | undefined) =>
  value == null ? "—" : `${Math.round(value * 100)}%`;

export function FieldDrawer({ fieldId, week, onClose }: Props) {
  const detail = useQuery({ queryKey: ["field", fieldId, week], queryFn: () => api.field(fieldId, week) });
  const history = useQuery({ queryKey: ["predictions", fieldId], queryFn: () => api.predictions(fieldId) });
  const signals = useQuery({ queryKey: ["signals", fieldId], queryFn: () => api.signals(fieldId) });

  const field = detail.data?.field;
  const option = {
    animation: false,
    tooltip: { trigger: "axis" },
    legend: { top: 0, textStyle: { color: "#526158", fontSize: 10 } },
    grid: { left: 38, right: 12, top: 38, bottom: 28 },
    xAxis: {
      type: "category",
      data: signals.data?.s2.map((row) => row.start_date) ?? [],
      axisLabel: { hideOverlap: true, color: "#738078" },
    },
    yAxis: { type: "value", axisLabel: { color: "#738078" }, splitLine: { lineStyle: { color: "#e4e8e1" } } },
    series: [
      { name: "NDVI", type: "line", symbol: "none", data: signals.data?.s2.map((row) => row.ndvi_mean), lineStyle: { color: "#3f8f59" } },
      { name: "EVI", type: "line", symbol: "none", data: signals.data?.s2.map((row) => row.evi_mean), lineStyle: { color: "#c58537" } },
      { name: "LAI", type: "line", symbol: "none", data: signals.data?.s2.map((row) => row.lai_mean), lineStyle: { color: "#367fba" }, yAxisIndex: 0 },
    ],
  };

  const selectedSignal = signals.data?.s2.find((row) => row.start_date === week);
  return (
    <aside className="field-drawer" aria-live="polite">
      <header>
        <div>
          <span className="eyebrow">Field observation</span>
          <h2>{fieldId}</h2>
        </div>
        <button onClick={onClose} aria-label="Close field details">×</button>
      </header>
      {detail.isLoading && <p className="muted">Loading field…</p>}
      {detail.error && <p className="error">Unable to load this field.</p>}
      {field && (
        <>
          <div className="field-meta">
            <span>{field.county}</span>
            <span>{field.area_ha == null ? "Area unavailable" : `${field.area_ha.toFixed(2)} ha`}</span>
            <span>{week}</span>
          </div>
          <div className={`prediction-card label-${field.label.toLowerCase()}`}>
            <span>Model call</span>
            <strong>{field.label}</strong>
            <div><b>{percentage(field.confidence)}</b> confidence</div>
            <small>{field.reason.replaceAll("_", " ")}</small>
          </div>
          <div className="probability-grid">
            <div><span>Maize</span><strong>{percentage(field.p_maize)}</strong></div>
            <div><span>Beans</span><strong>{percentage(field.p_beans)}</strong></div>
            <div><span>Potatoes</span><strong>{percentage(field.p_potatoes)}</strong></div>
          </div>
          <section className="quality-card">
            <h3>Pixel audit</h3>
            <dl>
              <div><dt>Buffer used</dt><dd>{selectedSignal?.buffer_width_m ?? "—"} m</dd></div>
              <div><dt>Before buffer</dt><dd>{selectedSignal?.pixel_count_before ?? "—"}</dd></div>
              <div><dt>After buffer</dt><dd>{selectedSignal?.pixel_count_after ?? "—"}</dd></div>
            </dl>
          </section>
          <section className="chart-card">
            <div className="section-title"><h3>Vegetation signals</h3><span>Weekly means</span></div>
            {signals.isLoading ? <p className="muted">Loading signals…</p> : <ReactECharts option={option} style={{ height: 230 }} />}
          </section>
          <section className="history-card">
            <div className="section-title"><h3>Prediction history</h3><span>{history.data?.predictions.length ?? 0} weeks</span></div>
            <div className="history-list">
              {history.data?.predictions.slice(-8).reverse().map((item) => (
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
