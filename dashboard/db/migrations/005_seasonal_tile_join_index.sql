CREATE INDEX IF NOT EXISTS seasonal_predictions_tile_join_idx
    ON dashboard.seasonal_predictions
       (field_id, model_run_id, season_code)
    INCLUDE (county, seasonal_label, status, seasonal_confidence);
