import type {
  Catalog,
  FieldDetail,
  Filters,
  Prediction,
  SignalRow,
  SummaryGroup,
  Week,
} from "./types";

const API_BASE = import.meta.env.VITE_API_BASE ?? "";

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || `Request failed: ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export const api = {
  catalog: () => getJson<Catalog>("/api/v1/catalog"),
  weeks: () => getJson<{ weeks: Week[] }>("/api/v1/weeks"),
  summary: (filters: Filters) => {
    const params = new URLSearchParams({ week: filters.week });
    if (filters.county) params.set("county", filters.county);
    if (filters.label) params.set("label", filters.label);
    if (filters.reason) params.set("reason", filters.reason);
    if (filters.minConfidence > 0) params.set("min_confidence", String(filters.minConfidence));
    return getJson<{ groups: SummaryGroup[] }>(`/api/v1/summary?${params}`);
  },
  field: (fieldId: string, week: string) =>
    getJson<{ field: FieldDetail }>(
      `/api/v1/fields/${encodeURIComponent(fieldId)}?week=${encodeURIComponent(week)}`,
    ),
  predictions: (fieldId: string) =>
    getJson<{ predictions: Prediction[] }>(
      `/api/v1/fields/${encodeURIComponent(fieldId)}/predictions`,
    ),
  signals: (fieldId: string) =>
    getJson<{ s1: SignalRow[]; s2: SignalRow[] }>(
      `/api/v1/fields/${encodeURIComponent(fieldId)}/signals`,
    ),
  search: (query: string) =>
    getJson<{ fields: Array<{ field_id: string; county: string; area_ha: number | null }> }>(
      `/api/v1/search/fields?q=${encodeURIComponent(query)}`,
    ),
};

export function tileUrl(filters: {
  week: string;
  county: string;
  label: string;
  reason: string;
  minConfidence: number;
}): string {
  const params = new URLSearchParams({ week: filters.week });
  if (filters.county) params.set("county", filters.county);
  if (filters.label) params.set("label", filters.label);
  if (filters.reason) params.set("reason", filters.reason);
  if (filters.minConfidence > 0) {
    params.set("min_confidence", String(filters.minConfidence));
  }
  return `${window.location.origin}/api/v1/tiles/fields/{z}/{x}/{y}.mvt?${params}`;
}
