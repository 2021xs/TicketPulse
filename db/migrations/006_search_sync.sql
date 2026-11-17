-- M2-01. Requires migrations 001-005. Re-runnable: creates only new control tables.
-- No existing business tables, tickets, histories or login data are rewritten.
CREATE TABLE IF NOT EXISTS sync_cdc_checkpoint (
    source_name VARCHAR(64) NOT NULL PRIMARY KEY,
    server_uuid VARCHAR(64) NOT NULL,
    binlog_file VARCHAR(255) NOT NULL,
    binlog_pos BIGINT UNSIGNED NOT NULL,
    bootstrap_complete BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sync_worker_state (
    component VARCHAR(64) NOT NULL PRIMARY KEY,
    state VARCHAR(32) NOT NULL,
    heartbeat_at DATETIME(6) NOT NULL,
    last_event_at DATETIME(6) NULL,
    last_error VARCHAR(1024) NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sync_deliveries (
    stream_sequence BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    ticket_id BIGINT NULL,
    ticket_version BIGINT NULL,
    status VARCHAR(32) NOT NULL,
    attempts INT UNSIGNED NOT NULL,
    first_seen_at DATETIME(6) NOT NULL,
    last_seen_at DATETIME(6) NOT NULL,
    event_at DATETIME(6) NULL,
    last_error VARCHAR(1024) NULL,
    isolated_payload MEDIUMBLOB NULL,
    recovery_sequence BIGINT UNSIGNED NULL,
    INDEX idx_sync_status(status,last_seen_at),
    INDEX idx_sync_event(event_id)
) ENGINE=InnoDB;
