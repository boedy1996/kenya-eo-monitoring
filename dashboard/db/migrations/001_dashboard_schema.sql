CREATE SCHEMA IF NOT EXISTS dashboard;
CREATE SCHEMA IF NOT EXISTS timeseries;

CREATE TABLE IF NOT EXISTS dashboard.schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dashboard.fields (
    feature_id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    field_id text PRIMARY KEY,
    county text NOT NULL CHECK (county IN ('Machakos', 'Nakuru')),
    area_ha double precision,
    source_layer text NOT NULL,
    geom geometry(MultiPolygon, 3857) NOT NULL,
    geom_simplified geometry(MultiPolygon, 3857) NOT NULL,
    centroid geometry(Point, 3857) NOT NULL
);

CREATE INDEX IF NOT EXISTS fields_county_idx
    ON dashboard.fields (county);
CREATE INDEX IF NOT EXISTS fields_geom_gix
    ON dashboard.fields USING gist (geom);
CREATE INDEX IF NOT EXISTS fields_geom_simplified_gix
    ON dashboard.fields USING gist (geom_simplified);
CREATE INDEX IF NOT EXISTS fields_centroid_gix
    ON dashboard.fields USING gist (centroid);

CREATE TABLE IF NOT EXISTS dashboard.model_runs (
    model_run_id text PRIMARY KEY,
    source_path text,
    date_from date NOT NULL,
    date_to date NOT NULL,
    field_count integer NOT NULL CHECK (field_count > 0),
    week_count integer NOT NULL CHECK (week_count > 0),
    prediction_rows bigint NOT NULL CHECK (prediction_rows > 0),
    is_active boolean NOT NULL DEFAULT false,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS model_runs_one_active_idx
    ON dashboard.model_runs (is_active)
    WHERE is_active;

CREATE TABLE IF NOT EXISTS dashboard.crop_predictions (
    model_run_id text NOT NULL,
    field_id text NOT NULL,
    week_start date NOT NULL,
    county text NOT NULL,
    season text,
    week_in_season smallint,
    scorable boolean NOT NULL,
    label text NOT NULL,
    reason text NOT NULL,
    confidence double precision,
    p_maize double precision,
    p_beans double precision,
    p_potatoes double precision,
    PRIMARY KEY (model_run_id, field_id, week_start)
) PARTITION BY RANGE (week_start);

DO $partitions$
DECLARE
    year_value integer;
BEGIN
    FOR year_value IN 2016..2030 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS dashboard.crop_predictions_%s '
            'PARTITION OF dashboard.crop_predictions '
            'FOR VALUES FROM (%L) TO (%L)',
            year_value,
            make_date(year_value, 1, 1),
            make_date(year_value + 1, 1, 1)
        );
    END LOOP;
END
$partitions$;

CREATE INDEX IF NOT EXISTS crop_predictions_week_field_idx
    ON dashboard.crop_predictions (week_start, field_id);
CREATE INDEX IF NOT EXISTS crop_predictions_field_week_idx
    ON dashboard.crop_predictions (field_id, week_start);
CREATE INDEX IF NOT EXISTS crop_predictions_week_county_label_idx
    ON dashboard.crop_predictions (week_start, county, label);
CREATE INDEX IF NOT EXISTS crop_predictions_week_reason_idx
    ON dashboard.crop_predictions (week_start, reason);

CREATE TABLE IF NOT EXISTS dashboard.prediction_week_summary (
    model_run_id text NOT NULL,
    week_start date NOT NULL,
    county text NOT NULL,
    season text,
    week_in_season smallint,
    label text NOT NULL,
    reason text NOT NULL,
    field_count integer NOT NULL,
    mean_confidence double precision
);

CREATE INDEX IF NOT EXISTS prediction_week_summary_lookup_idx
    ON dashboard.prediction_week_summary
       (model_run_id, week_start, county, label);

CREATE TABLE IF NOT EXISTS timeseries.s1_indices (
    field_id text NOT NULL,
    region text,
    layer_name text,
    source text,
    raster_file text,
    start_date date NOT NULL,
    end_date date,
    iso_year smallint,
    week_no smallint,
    buffer_width_m smallint,
    pixel_count_before integer,
    pixel_count_after integer,
    below_min_pixel_threshold boolean,
    sigma_vv_linear_mean real,
    sigma_vv_linear_median real,
    sigma_vv_linear_min real,
    sigma_vv_linear_max real,
    sigma_vv_linear_std real,
    sigma_vv_linear_count integer,
    sigma_vh_linear_mean real,
    sigma_vh_linear_median real,
    sigma_vh_linear_min real,
    sigma_vh_linear_max real,
    sigma_vh_linear_std real,
    sigma_vh_linear_count integer,
    p_ratio_mean real,
    p_ratio_median real,
    p_ratio_min real,
    p_ratio_max real,
    p_ratio_std real,
    p_ratio_count integer,
    rvi_mean real,
    rvi_median real,
    rvi_min real,
    rvi_max real,
    rvi_std real,
    rvi_count integer,
    rcspr_mean real,
    rcspr_median real,
    rcspr_min real,
    rcspr_max real,
    rcspr_std real,
    rcspr_count integer,
    PRIMARY KEY (field_id, start_date)
);

CREATE INDEX IF NOT EXISTS s1_indices_start_date_idx
    ON timeseries.s1_indices (start_date, field_id);

CREATE TABLE IF NOT EXISTS timeseries.s2_indices (
    field_id text NOT NULL,
    region text,
    layer_name text,
    source text,
    raster_file text,
    start_date date NOT NULL,
    end_date date,
    iso_year smallint,
    week_no smallint,
    buffer_width_m smallint,
    pixel_count_before integer,
    pixel_count_after integer,
    below_min_pixel_threshold boolean,
    ndvi_mean real,
    ndvi_median real,
    ndvi_min real,
    ndvi_max real,
    ndvi_std real,
    ndvi_count integer,
    evi_mean real,
    evi_median real,
    evi_min real,
    evi_max real,
    evi_std real,
    evi_count integer,
    ndmi_mean real,
    ndmi_median real,
    ndmi_min real,
    ndmi_max real,
    ndmi_std real,
    ndmi_count integer,
    ndre_mean real,
    ndre_median real,
    ndre_min real,
    ndre_max real,
    ndre_std real,
    ndre_count integer,
    msi_mean real,
    msi_median real,
    msi_min real,
    msi_max real,
    msi_std real,
    msi_count integer,
    pvr_mean real,
    pvr_median real,
    pvr_min real,
    pvr_max real,
    pvr_std real,
    pvr_count integer,
    lai_mean real,
    lai_median real,
    lai_min real,
    lai_max real,
    lai_std real,
    lai_count integer,
    PRIMARY KEY (field_id, start_date)
);

CREATE INDEX IF NOT EXISTS s2_indices_start_date_idx
    ON timeseries.s2_indices (start_date, field_id);
