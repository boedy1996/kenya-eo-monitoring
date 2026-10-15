CREATE INDEX IF NOT EXISTS seasonal_predictions_tile_idx
    ON dashboard.seasonal_predictions
       (model_run_id, season_code, county, seasonal_label, status)
    INCLUDE (field_id, seasonal_confidence);

DROP INDEX IF EXISTS dashboard.seasonal_predictions_map_idx;
