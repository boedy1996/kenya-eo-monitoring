export type ModelRun = {
  model_run_id: string;
  date_from: string;
  date_to: string;
  field_count: number;
  week_count: number;
  prediction_rows: number;
  loaded_at: string;
};

export type Catalog = {
  model_run: ModelRun;
  counties: string[];
  labels: string[];
  reasons: string[];
  seasons: string[];
};

export type Week = {
  week_start: string;
  season: string | null;
  week_in_season: number | null;
};

export type SeasonDefinition = {
  season_code: string;
  season_year: number;
  season_kind: "lr" | "sr";
  season_name: string;
  season_start: string;
  season_end: string;
  is_complete: boolean;
  method_version: string;
};

export type Prediction = {
  week_start: string;
  county: string;
  season: string | null;
  week_in_season: number | null;
  scorable: boolean;
  label: string;
  reason: string;
  confidence: number | null;
  p_maize: number | null;
  p_beans: number | null;
  p_potatoes: number | null;
};

export type FieldDetail = Prediction & {
  field_id: string;
  area_ha: number | null;
  source_layer: string;
  longitude: number;
  latitude: number;
};

export type SeasonalFieldDetail = {
  field_id: string;
  county: string;
  area_ha: number | null;
  source_layer: string;
  longitude: number;
  latitude: number;
  season_code: string;
  season_year: number;
  season_kind: "lr" | "sr";
  season_start: string;
  season_end: string;
  seasonal_label: string;
  leading_label: string | null;
  seasonal_confidence: number | null;
  status: "final" | "provisional" | "ambiguous" | "insufficient_data";
  total_weeks: number;
  classified_weeks: number;
  appearance_count: number | null;
  vote_share: number | null;
  median_confidence: number | null;
  maximum_confidence: number | null;
  runner_up_label: string | null;
  runner_up_confidence: number | null;
  confidence_margin: number | null;
  method_version: string;
};

export type SummaryGroup = {
  county: string;
  season: string | null;
  week_in_season: number | null;
  label: string;
  reason?: string;
  field_count: number;
  mean_confidence: number | null;
  status?: string;
};

export type SignalRow = {
  start_date: string;
  buffer_width_m: number | null;
  pixel_count_before: number | null;
  pixel_count_after: number | null;
  below_min_pixel_threshold: boolean | null;
  [key: string]: string | number | boolean | null;
};

export type Filters = {
  mode: "weekly" | "seasonal";
  week: string;
  season: string;
  county: string;
  label: string;
  reason: string;
  minConfidence: number;
};
