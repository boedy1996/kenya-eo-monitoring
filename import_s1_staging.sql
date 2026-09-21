\timing on
\set ON_ERROR_STOP on

\echo 'Starting S1 CSV import...'

\copy staging.s1_indices_raw (uid,region,layer_name,source,raster_file,start_date,end_date,iso_year,week_no,sigma_vv_linear_mean,sigma_vv_linear_median,sigma_vv_linear_min,sigma_vv_linear_max,sigma_vv_linear_std,sigma_vv_linear_count,sigma_vh_linear_mean,sigma_vh_linear_median,sigma_vh_linear_min,sigma_vh_linear_max,sigma_vh_linear_std,sigma_vh_linear_count,p_ratio_mean,p_ratio_median,p_ratio_min,p_ratio_max,p_ratio_std,p_ratio_count,rvi_mean,rvi_median,rvi_min,rvi_max,rvi_std,rvi_count,rcspr_mean,rcspr_median,rcspr_min,rcspr_max,rcspr_std,rcspr_count) FROM '/opt/project/kenya_s1_ops/zonal_stats_s1.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8');

\echo 'S1 CSV import finished.'

SELECT COUNT(*) AS imported_rows
FROM staging.s1_indices_raw;
