\set ON_ERROR_STOP on
COPY (
    SELECT
        region,
        raster_file,
        start_date,
        end_date,
        count(*) AS field_records,
        count(p_ratio_mean) AS valid_field_records,
        count(*) FILTER (
            WHERE sigma_vv_linear_mean IS NOT NULL
              AND sigma_vh_linear_mean IS NOT NULL
              AND sigma_vv_linear_mean < sigma_vh_linear_mean
        ) AS fields_band1_lower_than_band2,
        count(*) FILTER (WHERE p_ratio_mean > 1.0) AS fields_p_ratio_gt_1,
        avg(sigma_vv_linear_mean) AS labelled_vv_mean,
        avg(sigma_vh_linear_mean) AS labelled_vh_mean,
        avg(p_ratio_mean) AS p_ratio_mean,
        avg(rvi_mean) AS rvi_mean,
        avg(rcspr_mean) AS rcspr_mean
    FROM timeseries.s1_indices
    GROUP BY region, raster_file, start_date, end_date
    ORDER BY start_date, region
) TO '/opt/project/s1_bandfix_runs/audit-20260928/all_weeks_db_audit.csv' WITH (FORMAT CSV, HEADER TRUE);
