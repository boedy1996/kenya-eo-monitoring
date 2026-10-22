\set ON_ERROR_STOP on

-- Run with psql --csv and redirect stdout to preserve this audit report.
WITH grouped AS (
    SELECT
        CASE WHEN GROUPING(region) = 1 THEN 'overall' ELSE 'year_region' END AS scope,
        region,
        iso_year,
        count(*) AS field_records,
        count(*) FILTER (
            WHERE sigma_vv_linear_mean IS NOT NULL
              AND sigma_vh_linear_mean IS NOT NULL
        ) AS dual_pol_records,
        count(*) FILTER (
            WHERE sigma_vv_linear_mean IS NOT NULL
              AND sigma_vh_linear_mean IS NOT NULL
              AND sigma_vv_linear_mean < sigma_vh_linear_mean
        ) AS vv_below_vh_records,
        count(*) FILTER (WHERE p_ratio_mean > 1.0) AS p_ratio_gt_1_records,
        count(*) FILTER (
            WHERE p_ratio_mean < 0.0
               OR rvi_mean < 0.0 OR rvi_mean > 4.0
               OR rcspr_mean < -1.0 OR rcspr_mean > 1.0
        ) AS bad_index_domain,
        count(*) FILTER (
            WHERE rvi_mean IS NOT NULL
              AND rcspr_mean IS NOT NULL
              AND abs(rvi_mean - 2.0 * (1.0 - rcspr_mean)) > 0.0001
        ) AS bad_rvi_rcspr_identity,
        count(*) FILTER (
            WHERE sigma_vv_linear_mean IS NOT NULL
              AND sigma_vh_linear_mean IS NULL
        ) AS vv_only_records,
        count(*) FILTER (
            WHERE sigma_vh_linear_mean IS NULL
              AND (p_ratio_mean IS NOT NULL OR rvi_mean IS NOT NULL OR rcspr_mean IS NOT NULL)
        ) AS vv_only_with_derived_value,
        avg(p_ratio_mean) AS mean_p_ratio,
        avg(rvi_mean) AS mean_rvi,
        avg(rcspr_mean) AS mean_rcspr
    FROM timeseries.s1_indices
    GROUP BY GROUPING SETS ((), (region, iso_year))
)
SELECT *
FROM grouped
ORDER BY scope, region NULLS FIRST, iso_year NULLS FIRST;
