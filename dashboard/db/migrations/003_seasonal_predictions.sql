CREATE TABLE IF NOT EXISTS dashboard.season_definitions (
    model_run_id text NOT NULL,
    season_code text NOT NULL,
    season_year smallint NOT NULL,
    season_kind text NOT NULL CHECK (season_kind IN ('lr', 'sr')),
    season_name text NOT NULL,
    season_start date NOT NULL,
    season_end date NOT NULL,
    is_complete boolean NOT NULL,
    method_version text NOT NULL,
    built_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model_run_id, season_code)
);

CREATE TABLE IF NOT EXISTS dashboard.seasonal_predictions (
    model_run_id text NOT NULL,
    field_id text NOT NULL,
    county text NOT NULL,
    season_code text NOT NULL,
    season_year smallint NOT NULL,
    season_kind text NOT NULL CHECK (season_kind IN ('lr', 'sr')),
    season_start date NOT NULL,
    season_end date NOT NULL,
    seasonal_label text NOT NULL,
    leading_label text,
    seasonal_confidence double precision,
    status text NOT NULL CHECK (
        status IN ('final', 'provisional', 'ambiguous', 'insufficient_data')
    ),
    total_weeks smallint NOT NULL,
    classified_weeks smallint NOT NULL,
    appearance_count smallint,
    vote_share double precision,
    median_confidence double precision,
    maximum_confidence double precision,
    runner_up_label text,
    runner_up_confidence double precision,
    confidence_margin double precision,
    method_version text NOT NULL,
    PRIMARY KEY (model_run_id, field_id, season_code)
);

CREATE INDEX IF NOT EXISTS seasonal_predictions_map_idx
    ON dashboard.seasonal_predictions
       (model_run_id, season_code, county, seasonal_label, status);
CREATE INDEX IF NOT EXISTS seasonal_predictions_field_idx
    ON dashboard.seasonal_predictions
       (field_id, season_start);

CREATE TABLE IF NOT EXISTS dashboard.seasonal_prediction_summary (
    model_run_id text NOT NULL,
    season_code text NOT NULL,
    county text NOT NULL,
    label text NOT NULL,
    status text NOT NULL,
    field_count integer NOT NULL,
    mean_confidence double precision,
    PRIMARY KEY (model_run_id, season_code, county, label, status)
);

CREATE INDEX IF NOT EXISTS seasonal_prediction_summary_lookup_idx
    ON dashboard.seasonal_prediction_summary
       (model_run_id, season_code, county, label, status);

CREATE TABLE IF NOT EXISTS dashboard.seasonal_build_checkpoints (
    model_run_id text NOT NULL,
    season_code text NOT NULL,
    method_version text NOT NULL,
    row_count integer NOT NULL CHECK (row_count > 0),
    built_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model_run_id, season_code)
);
