CREATE TABLE IF NOT EXISTS dashboard.load_checkpoints (
    model_run_id text NOT NULL,
    source_file text NOT NULL,
    source_size_bytes bigint NOT NULL CHECK (source_size_bytes > 0),
    row_count bigint NOT NULL CHECK (row_count > 0),
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (model_run_id, source_file)
);

CREATE INDEX IF NOT EXISTS load_checkpoints_run_idx
    ON dashboard.load_checkpoints (model_run_id, loaded_at);
