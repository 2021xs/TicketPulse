-- M4-01: only additive control data, no business or existing projection is changed.
CREATE TABLE IF NOT EXISTS sync_rebuild_jobs (
    job_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    target VARCHAR(8) NOT NULL,
    options JSON NOT NULL,
    es_index VARCHAR(201) NOT NULL,
    ck_database VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    phase VARCHAR(32) NOT NULL,
    source_uuid VARCHAR(64) NULL,
    source_position JSON NULL,
    topic_id VARCHAR(64) NULL,
    start_offsets JSON NULL,
    target_identity JSON NULL,
    source_sha256 VARCHAR(64) NULL,
    total BIGINT NOT NULL DEFAULT 0,
    applied BIGINT NOT NULL DEFAULT 0,
    attempts INT NOT NULL DEFAULT 0,
    barrier_id VARCHAR(64) NULL,
    report JSON NULL,
    last_error VARCHAR(1024) NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    completed_at DATETIME(6) NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_rebuild_items (
    job_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    sequence_id BIGINT NOT NULL,
    payload JSON NOT NULL,
    PRIMARY KEY(job_id,sequence_id),
    FOREIGN KEY(job_id) REFERENCES sync_rebuild_jobs(job_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_rebuild_offsets (
    job_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    partition_id INT NOT NULL,
    next_offset BIGINT NOT NULL,
    barrier_id VARCHAR(64) NULL,
    PRIMARY KEY(job_id,partition_id),
    FOREIGN KEY(job_id) REFERENCES sync_rebuild_jobs(job_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_rebuild_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    phase VARCHAR(32) NOT NULL,
    outcome VARCHAR(32) NOT NULL,
    details JSON NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    KEY idx_rebuild_log(job_id,id)
) ENGINE=InnoDB;
