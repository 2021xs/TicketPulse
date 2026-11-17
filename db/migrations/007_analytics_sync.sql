-- M3-01. Requires 006; re-runnable, preserves all existing business/ES control data.
-- CK delivery records are separate from ES records; never share a terminal disposition.
CREATE TABLE IF NOT EXISTS sync_ck_deliveries LIKE sync_deliveries;

CREATE TABLE IF NOT EXISTS sync_source_watermark (
    source_name VARCHAR(64) NOT NULL PRIMARY KEY,
    barrier_id VARCHAR(64) NOT NULL,
    issued_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sync_consumer_watermarks (
    component VARCHAR(64) NOT NULL PRIMARY KEY,
    barrier_id VARCHAR(64) NOT NULL,
    issued_at DATETIME(6) NOT NULL,
    stream_sequence BIGINT UNSIGNED NOT NULL,
    binlog_file VARCHAR(255) NOT NULL,
    binlog_pos BIGINT UNSIGNED NOT NULL,
    applied_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sync_analytics_bootstrap (
    pipeline VARCHAR(64) NOT NULL PRIMARY KEY,
    end_sequence BIGINT UNSIGNED NOT NULL,
    published_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;
