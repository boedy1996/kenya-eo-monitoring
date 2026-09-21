BEGIN;

-- ============================================================
-- 1. SCHEMAS
-- ============================================================

CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS timeseries;
CREATE SCHEMA IF NOT EXISTS metadata;


-- ============================================================
-- 2. REFERENCE FIELD TABLE
-- Geometry akan diimpor pada tahap berikutnya.
-- field_id menjadi kunci numerik untuk join S1 dan S2.
-- ============================================================

CREATE TABLE IF NOT EXISTS reference.fields (
    field_id       bigint GENERATED ALWAYS AS IDENTITY,
    uid            text NOT NULL,
    region         text NOT NULL,
    layer_name     text,
    geom           geometry(MultiPolygon, 4326),

    CONSTRAINT fields_pk
        PRIMARY KEY (field_id),

    CONSTRAINT fields_region_uid_uq
        UNIQUE (region, uid)
);

CREATE INDEX IF NOT EXISTS fields_region_uid_idx
    ON reference.fields (region, uid);

CREATE INDEX IF NOT EXISTS fields_geom_gix
    ON reference.fields
    USING GIST (geom);


-- ============================================================
-- 3. STAGING TABLE S1
-- Mengikuti struktur CSV zonal statistics.
-- UNLOGGED dipakai karena ini hanya tabel sementara.
-- ============================================================

CREATE UNLOGGED TABLE IF NOT EXISTS staging.s1_indices_raw (
    uid                         text,
    region                      text,
    layer_name                  text,
    source                      text,
    raster_file                 text,
    start_date                  date,
    end_date                    date,
    iso_year                    smallint,
    week_no                     smallint,

    sigma_vv_linear_mean        real,
    sigma_vv_linear_median      real,
    sigma_vv_linear_min         real,
    sigma_vv_linear_max         real,
    sigma_vv_linear_std         real,
    sigma_vv_linear_count       integer,

    sigma_vh_linear_mean        real,
    sigma_vh_linear_median      real,
    sigma_vh_linear_min         real,
    sigma_vh_linear_max         real,
    sigma_vh_linear_std         real,
    sigma_vh_linear_count       integer,

    p_ratio_mean                real,
    p_ratio_median              real,
    p_ratio_min                 real,
    p_ratio_max                 real,
    p_ratio_std                 real,
    p_ratio_count               integer,

    rvi_mean                    real,
    rvi_median                  real,
    rvi_min                     real,
    rvi_max                     real,
    rvi_std                     real,
    rvi_count                   integer,

    rcspr_mean                  real,
    rcspr_median                real,
    rcspr_min                   real,
    rcspr_max                   real,
    rcspr_std                   real,
    rcspr_count                 integer
);


-- ============================================================
-- 4. STAGING TABLE S2
-- ============================================================

CREATE UNLOGGED TABLE IF NOT EXISTS staging.s2_indices_raw (
    uid                         text,
    region                      text,
    layer_name                  text,
    source                      text,
    raster_file                 text,
    start_date                  date,
    end_date                    date,
    iso_year                    smallint,
    week_no                     smallint,

    ndvi_mean                   real,
    ndvi_median                 real,
    ndvi_min                    real,
    ndvi_max                    real,
    ndvi_std                    real,
    ndvi_count                  integer,

    evi_mean                    real,
    evi_median                  real,
    evi_min                     real,
    evi_max                     real,
    evi_std                     real,
    evi_count                   integer,

    ndmi_mean                   real,
    ndmi_median                 real,
    ndmi_min                    real,
    ndmi_max                    real,
    ndmi_std                    real,
    ndmi_count                  integer,

    ndre_mean                   real,
    ndre_median                 real,
    ndre_min                    real,
    ndre_max                    real,
    ndre_std                    real,
    ndre_count                  integer,

    msi_mean                    real,
    msi_median                  real,
    msi_min                     real,
    msi_max                     real,
    msi_std                     real,
    msi_count                   integer,

    pvr_mean                    real,
    pvr_median                  real,
    pvr_min                     real,
    pvr_max                     real,
    pvr_std                     real,
    pvr_count                   integer,

    lai_mean                    real,
    lai_median                  real,
    lai_min                     real,
    lai_max                     real,
    lai_std                     real,
    lai_count                   integer
);


-- ============================================================
-- 5. FINAL PARTITIONED TABLE S1
-- PK(field_id, start_date) mendukung:
-- - satu field sepanjang waktu
-- - join S1 dengan S2
-- ============================================================

CREATE TABLE IF NOT EXISTS timeseries.s1_indices (
    field_id                    bigint NOT NULL,
    region                      text NOT NULL,
    layer_name                  text,
    raster_file                 text NOT NULL,
    start_date                  date NOT NULL,
    end_date                    date NOT NULL,
    iso_year                    smallint NOT NULL,
    week_no                     smallint NOT NULL,

    sigma_vv_linear_mean        real,
    sigma_vv_linear_median      real,
    sigma_vv_linear_min         real,
    sigma_vv_linear_max         real,
    sigma_vv_linear_std         real,
    sigma_vv_linear_count       integer,

    sigma_vh_linear_mean        real,
    sigma_vh_linear_median      real,
    sigma_vh_linear_min         real,
    sigma_vh_linear_max         real,
    sigma_vh_linear_std         real,
    sigma_vh_linear_count       integer,

    p_ratio_mean                real,
    p_ratio_median              real,
    p_ratio_min                 real,
    p_ratio_max                 real,
    p_ratio_std                 real,
    p_ratio_count               integer,

    rvi_mean                    real,
    rvi_median                  real,
    rvi_min                     real,
    rvi_max                     real,
    rvi_std                     real,
    rvi_count                   integer,

    rcspr_mean                  real,
    rcspr_median                real,
    rcspr_min                   real,
    rcspr_max                   real,
    rcspr_std                   real,
    rcspr_count                 integer,

    CONSTRAINT s1_indices_pk
        PRIMARY KEY (field_id, start_date),

    CONSTRAINT s1_week_no_chk
        CHECK (week_no BETWEEN 1 AND 53),

    CONSTRAINT s1_date_range_chk
        CHECK (end_date >= start_date)

) PARTITION BY RANGE (start_date);


-- ============================================================
-- 6. FINAL PARTITIONED TABLE S2
-- ============================================================

CREATE TABLE IF NOT EXISTS timeseries.s2_indices (
    field_id                    bigint NOT NULL,
    region                      text NOT NULL,
    layer_name                  text,
    raster_file                 text NOT NULL,
    start_date                  date NOT NULL,
    end_date                    date NOT NULL,
    iso_year                    smallint NOT NULL,
    week_no                     smallint NOT NULL,

    ndvi_mean                   real,
    ndvi_median                 real,
    ndvi_min                    real,
    ndvi_max                    real,
    ndvi_std                    real,
    ndvi_count                  integer,

    evi_mean                    real,
    evi_median                  real,
    evi_min                     real,
    evi_max                     real,
    evi_std                     real,
    evi_count                   integer,

    ndmi_mean                   real,
    ndmi_median                 real,
    ndmi_min                    real,
    ndmi_max                   real,
    ndmi_std                    real,
    ndmi_count                  integer,

    ndre_mean                   real,
    ndre_median                 real,
    ndre_min                    real,
    ndre_max                    real,
    ndre_std                    real,
    ndre_count                  integer,

    msi_mean                    real,
    msi_median                 real,
    msi_min                    real,
    msi_max                    real,
    msi_std                    real,
    msi_count                  integer,

    pvr_mean                    real,
    pvr_median                 real,
    pvr_min                    real,
    pvr_max                    real,
    pvr_std                    real,
    pvr_count                  integer,

    lai_mean                    real,
    lai_median                 real,
    lai_min                    real,
    lai_max                    real,
    lai_std                    real,
    lai_count                  integer,

    CONSTRAINT s2_indices_pk
        PRIMARY KEY (field_id, start_date),

    CONSTRAINT s2_week_no_chk
        CHECK (week_no BETWEEN 1 AND 53),

    CONSTRAINT s2_date_range_chk
        CHECK (end_date >= start_date)

) PARTITION BY RANGE (start_date);


-- ============================================================
-- 7. YEARLY PARTITIONS
-- 2017 sampai 2027
-- ============================================================

DO $$
DECLARE
    y integer;
BEGIN
    FOR y IN 2016..2030 LOOP

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS timeseries.s1_indices_%s
             PARTITION OF timeseries.s1_indices
             FOR VALUES FROM (%L) TO (%L)',
            y,
            make_date(y, 1, 1),
            make_date(y + 1, 1, 1)
        );

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS timeseries.s2_indices_%s
             PARTITION OF timeseries.s2_indices
             FOR VALUES FROM (%L) TO (%L)',
            y,
            make_date(y, 1, 1),
            make_date(y + 1, 1, 1)
        );

    END LOOP;
END $$;


-- ============================================================
-- 8. SECONDARY INDEXES
-- PK sudah membuat index (field_id, start_date).
--
-- Index berikut mendukung:
-- - semua field pada satu minggu
-- - join atau modelling berdasarkan tanggal
-- ============================================================

CREATE INDEX IF NOT EXISTS s1_indices_date_field_idx
    ON timeseries.s1_indices (start_date, field_id);

CREATE INDEX IF NOT EXISTS s2_indices_date_field_idx
    ON timeseries.s2_indices (start_date, field_id);


-- Region + date hanya diperlukan jika query sering berdasarkan region.
CREATE INDEX IF NOT EXISTS s1_indices_region_date_idx
    ON timeseries.s1_indices (region, start_date);

CREATE INDEX IF NOT EXISTS s2_indices_region_date_idx
    ON timeseries.s2_indices (region, start_date);


COMMIT;
