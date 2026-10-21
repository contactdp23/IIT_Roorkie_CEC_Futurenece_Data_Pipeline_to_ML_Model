CREATE TABLE IF NOT EXISTS raw_ingestion_log (
    source_file VARCHAR NOT NULL,
    content_sha256 VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    run_id VARCHAR NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR NOT NULL
);
