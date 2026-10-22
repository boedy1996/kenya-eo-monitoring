import type {
  Catalog,
  FieldDetail,
  Filters,
  Prediction,
  SeasonDefinition,
  SeasonalFieldDetail,
  SignalRow,
  SummaryGroup,
  SeasonalYieldSummary,
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
  seasons: () => getJson<{ seasons: SeasonDefinition[] }>("/api/v1/seasons"),
  summary: (filters: Filters) => {
    if (filters.mode === "seasonal") {
      const params = new URLSearchParams({ season: filters.season });
      if (filters.county) params.set("county", filters.county);
      if (filters.label) params.set("label", filters.label);
      if (filters.reason) params.set("status", filters.reason);
      if (filters.minConfidence > 0) {
        params.set("min_confidence", String(filters.minConfidence));
      }
      return getJson<{ groups: SummaryGroup[] }>(`/api/v1/summary/seasonal?${params}`);
    }
    const params = new URLSearchParams({ week: filters.week });
    if (filters.county) params.set("county", filters.county);
    if (filters.label) params.set("label", filters.label);
    if (filters.reason) params.set("reason", filters.reason);
    if (filters.minConfidence > 0) params.set("min_confidence", String(filters.minConfidence));
    return getJson<{ groups: SummaryGroup[] }>(`/api/v1/summary?${params}`);
  },
  seasonalYieldSummary: (filters: Filters) => {
    const params = new URLSearchParams({ season: filters.season });
    if (filters.county) params.set("county", filters.county);
    if (filters.label) params.set("label", filters.label.toLowerCase());
    return getJson<SeasonalYieldSummary>(`/api/v1/summary/seasonal/yield?${params}`);
  },
  field: (fieldId: string, week: string) =>
    getJson<{ field: FieldDetail }>(
      `/api/v1/fields/${encodeURIComponent(fieldId)}?week=${encodeURIComponent(week)}`,
    ),
  seasonalField: (fieldId: string, season: string) =>
    getJson<{ field: SeasonalFieldDetail }>(
      `/api/v1/fields/${encodeURIComponent(fieldId)}/seasonal?season=${encodeURIComponent(season)}`,
    ),
  predictions: (fieldId: string, dateFrom?: string, dateTo?: string) => {
    const params = new URLSearchParams();
    if (dateFrom) params.set("date_from", dateFrom);
    if (dateTo) params.set("date_to", dateTo);
    const query = params.size ? `?${params}` : "";
    return getJson<{ predictions: Prediction[] }>(
      `/api/v1/fields/${encodeURIComponent(fieldId)}/predictions${query}`,
    );
  },
  signals: (fieldId: string, dateFrom?: string, dateTo?: string) => {
    const params = new URLSearchParams();
    if (dateFrom) params.set("date_from", dateFrom);
    if (dateTo) params.set("date_to", dateTo);
    const query = params.size ? `?${params}` : "";
    return getJson<{ s1: SignalRow[]; s2: SignalRow[] }>(
      `/api/v1/fields/${encodeURIComponent(fieldId)}/signals${query}`,
    );
  },
  search: (query: string) =>
    getJson<{ fields: Array<{ field_id: string; county: string; area_ha: number | null }> }>(
      `/api/v1/search/fields?q=${encodeURIComponent(query)}`,
    ),
};

export function tileUrl(filters: Filters): string {
  const params = new URLSearchParams(
    filters.mode === "seasonal" ? { season: filters.season } : { week: filters.week },
  );
  if (filters.county) params.set("county", filters.county);
  if (filters.label) params.set("label", filters.label);
  if (filters.reason) {
    params.set(filters.mode === "seasonal" ? "status" : "reason", filters.reason);
  }
  if (filters.minConfidence > 0) {
    params.set("min_confidence", String(filters.minConfidence));
  }
  const route = filters.mode === "seasonal" ? "seasonal" : "fields";
  return `${window.location.origin}/api/v1/tiles/${route}/{z}/{x}/{y}.mvt?${params}`;
}
