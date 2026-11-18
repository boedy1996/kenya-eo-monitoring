CREATE TABLE IF NOT EXISTS dashboard.seasonal_yield_county_summary (
    yield_run_id text NOT NULL,
    model_run_id text NOT NULL,
    season_code text NOT NULL,
    county text NOT NULL,
    crop_label text NOT NULL,
    calculated_fields integer NOT NULL CHECK (calculated_fields > 0),
    calculated_unvalidated_fields integer NOT NULL CHECK (calculated_unvalidated_fields >= 0),
    intercrop_unknown_fields integer NOT NULL CHECK (intercrop_unknown_fields >= 0),
    mapped_area_ha double precision NOT NULL CHECK (mapped_area_ha > 0),
    yield_central_mean double precision,
    yield_central_median double precision,
    yield_central_min double precision,
    yield_central_max double precision,
    production_low_t double precision,
    production_central_t double precision,
    production_high_t double precision,
    yield_low_area_weighted double precision,
    yield_central_area_weighted double precision,
    yield_high_area_weighted double precision,
    aggregation_method_version text NOT NULL,
    aggregation_code_commit text NOT NULL,
    aggregation_built_at timestamptz NOT NULL,
    PRIMARY KEY (yield_run_id, season_code, county, crop_label)
);

CREATE INDEX IF NOT EXISTS seasonal_yield_county_summary_lookup_idx
    ON dashboard.seasonal_yield_county_summary
       (yield_run_id, season_code, county, crop_label);
