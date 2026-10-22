\set ON_ERROR_STOP on

SELECT
    corrected.field_id,
    count(*) AS matched_weeks,
    count(*) FILTER (
        WHERE corrected.sigma_vv_linear_mean IS NOT NULL
          AND corrected.sigma_vh_linear_mean IS NOT NULL
          AND previous.sigma_vv_linear_mean IS NOT NULL
          AND previous.sigma_vh_linear_mean IS NOT NULL
    ) AS matched_dual_pol_weeks,
    avg(abs(corrected.sigma_vv_linear_mean - previous.sigma_vh_linear_mean)) AS vv_vs_previous_vh_mae,
    avg(abs(corrected.sigma_vh_linear_mean - previous.sigma_vv_linear_mean)) AS vh_vs_previous_vv_mae,
    avg(previous.p_ratio_mean) AS previous_mean_p_ratio,
    avg(corrected.p_ratio_mean) AS corrected_mean_p_ratio,
    avg(previous.rvi_mean) AS previous_mean_rvi,
    avg(corrected.rvi_mean) AS corrected_mean_rvi,
    avg(previous.rcspr_mean) AS previous_mean_rcspr,
    avg(corrected.rcspr_mean) AS corrected_mean_rcspr
FROM timeseries.s1_indices_legacy_pre_s1_bandfix_20260928 AS previous
JOIN timeseries.s1_indices AS corrected
  USING (field_id, start_date)
WHERE corrected.field_id IN ('MA1', 'NA2255', 'NA2397', 'NA2410', 'NA2817')
GROUP BY corrected.field_id
ORDER BY corrected.field_id;
