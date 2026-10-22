CREATE TABLE IF NOT EXISTS dashboard.yield_runs (
    yield_run_id text PRIMARY KEY,
    model_run_id text NOT NULL,
    method_version text NOT NULL,
    parameter_set_id text NOT NULL,
    season_count smallint NOT NULL CHECK (season_count > 0),
    field_count integer NOT NULL CHECK (field_count > 0),
    result_rows bigint NOT NULL CHECK (result_rows > 0),
    status text NOT NULL CHECK (status IN ('loading', 'validated', 'failed')),
    is_active boolean NOT NULL DEFAULT false,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS yield_runs_one_active_per_model_idx
    ON dashboard.yield_runs (model_run_id)
    WHERE is_active;

CREATE TABLE IF NOT EXISTS dashboard.seasonal_yield (
    yield_run_id text NOT NULL,
    model_run_id text NOT NULL,
    field_id text NOT NULL,
    county text NOT NULL,
    season_code text NOT NULL,
    season_start date NOT NULL,
    season_end date NOT NULL,
    area_ha double precision NOT NULL CHECK (area_ha > 0),
    area_method text NOT NULL,
    crop_label text NOT NULL,
    crop_confidence double precision,
    crop_status text,
    intercrop_status text NOT NULL,
    survey_crop_text text,
    survey_crop_match boolean,
    sos date,
    peak_date date,
    eos date,
    phenology_method text,
    phenology_confidence double precision,
    phenology_qa_status text,
    optical_valid_weeks smallint,
    sar_supported_weeks smallint,
    seasonal_npp_gC_m2 double precision,
    npp_valid_days smallint,
    npp_total_days smallint,
    npp_temporal_coverage double precision,
    npp_mean_spatial_coverage double precision,
    npp_mean_effective_pixels double precision,
    npp_periods_used smallint,
    tbp_tDM_ha double precision,
    yield_low_t_ha double precision,
    yield_central_t_ha double precision,
    yield_high_t_ha double precision,
    production_low_t double precision,
    production_central_t double precision,
    production_high_t double precision,
    phenology_narrow_central_yield_t_ha double precision,
    phenology_wide_central_yield_t_ha double precision,
    parameter_set_id text NOT NULL,
    method_version text NOT NULL,
    yield_status text NOT NULL CHECK (
        yield_status IN (
            'calculated_unvalidated',
            'calculated_unvalidated_intercrop_unknown',
            'not_calculated'
        )
    ),
    qa_flags text NOT NULL DEFAULT '',
    PRIMARY KEY (yield_run_id, field_id, season_code)
);

CREATE INDEX IF NOT EXISTS seasonal_yield_field_season_idx
    ON dashboard.seasonal_yield (field_id, model_run_id, season_code)
    INCLUDE (yield_run_id, yield_status, yield_central_t_ha);
CREATE INDEX IF NOT EXISTS seasonal_yield_summary_idx
    ON dashboard.seasonal_yield
       (yield_run_id, model_run_id, season_code, county, crop_label, yield_status)
    INCLUDE (yield_central_t_ha, production_central_t);

CREATE TABLE IF NOT EXISTS dashboard.seasonal_yield_summary (
    yield_run_id text NOT NULL,
    model_run_id text NOT NULL,
    season_code text NOT NULL,
    county text NOT NULL,
    crop_label text NOT NULL,
    yield_status text NOT NULL,
    field_count integer NOT NULL,
    calculated_fields integer NOT NULL,
    yield_central_mean double precision,
    yield_central_median double precision,
    yield_central_min double precision,
    yield_central_max double precision,
    production_central_t double precision,
    PRIMARY KEY (yield_run_id, season_code, county, crop_label, yield_status)
);

CREATE INDEX IF NOT EXISTS seasonal_yield_summary_lookup_idx
    ON dashboard.seasonal_yield_summary (yield_run_id, season_code, county, crop_label);

CREATE TABLE IF NOT EXISTS dashboard.yield_load_checkpoints (
    yield_run_id text NOT NULL,
    season_code text NOT NULL,
    source_file text NOT NULL,
    source_size_bytes bigint NOT NULL,
    row_count integer NOT NULL CHECK (row_count > 0),
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (yield_run_id, season_code)
);
