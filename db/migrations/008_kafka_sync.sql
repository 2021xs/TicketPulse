-- M2-02. Requires 006 and 007. Re-runnable; legacy records and all business data stay intact.
CREATE TABLE IF NOT EXISTS sync_kafka_cdc_checkpoint LIKE sync_cdc_checkpoint;
CREATE TABLE IF NOT EXISTS sync_kafka_pipeline (
    pipeline_name VARCHAR(64) NOT NULL PRIMARY KEY,
    topic VARCHAR(249) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    topic_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    partition_count INT NOT NULL,
    created_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_kafka_es_deliveries (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    topic VARCHAR(249) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    partition_id INT NOT NULL,
    record_offset BIGINT NOT NULL,
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
    recovery_partition INT NULL,
    recovery_offset BIGINT NULL,
    UNIQUE KEY uk_delivery_record(topic,partition_id,record_offset),
    KEY idx_delivery_status(status,last_seen_at),
    KEY idx_delivery_ticket(ticket_id,ticket_version),
    CONSTRAINT ck_delivery_offset CHECK (partition_id>=0 AND record_offset>=0)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_kafka_ck_deliveries LIKE sync_kafka_es_deliveries;
CREATE TABLE IF NOT EXISTS sync_kafka_watermarks (
    component VARCHAR(64) NOT NULL,
    partition_id INT NOT NULL,
    topic VARCHAR(249) NOT NULL,
    barrier_id VARCHAR(64) NOT NULL,
    issued_at DATETIME(6) NOT NULL,
    record_offset BIGINT NOT NULL,
    binlog_file VARCHAR(255) NOT NULL,
    binlog_pos BIGINT UNSIGNED NOT NULL,
    applied_at DATETIME(6) NOT NULL,
    PRIMARY KEY(component,partition_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_kafka_analytics_bootstrap (
    pipeline VARCHAR(64) NOT NULL PRIMARY KEY,
    topic VARCHAR(249) NOT NULL,
    end_offsets JSON NOT NULL,
    published_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;
