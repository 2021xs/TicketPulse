-- C2 adds process and transition evidence. 011/012 Contracts and 013 remain intact.
CREATE TABLE IF NOT EXISTS sync_projection_read_execution (
    operation_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    drain_scope JSON NULL,
    drain_scope_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    drained JSON NULL,
    drained_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    committed JSON NULL,
    committed_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    readiness JSON NULL,
    readiness_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    resumed JSON NULL,
    resumed_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    revision BIGINT NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    FOREIGN KEY(operation_id) REFERENCES sync_projection_read_switches(operation_id),
    CHECK(revision>0),
    CHECK((drain_scope IS NULL)=(drain_scope_sha256 IS NULL)),
    CHECK((drained IS NULL)=(drained_sha256 IS NULL)),
    CHECK((committed IS NULL)=(committed_sha256 IS NULL)),
    CHECK((readiness IS NULL)=(readiness_sha256 IS NULL)),
    CHECK((resumed IS NULL)=(resumed_sha256 IS NULL))
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_projection_api_boots (
    boot_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    version_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    read_revision BIGINT NOT NULL,
    identity JSON NOT NULL,
    identity_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    state VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    heartbeat_at DATETIME(6) NOT NULL,
    shutdown JSON NULL,
    exited_at DATETIME(6) NULL,
    FOREIGN KEY(version_id) REFERENCES sync_projection_versions(version_id),
    CHECK(read_revision>0),
    CHECK(state IN ('STARTING','RUNNING','DRAINING','EXITED'))
) ENGINE=InnoDB;
