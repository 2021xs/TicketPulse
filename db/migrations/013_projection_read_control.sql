-- C1 installs metadata only. Initialization is a separate frozen identity check.
CREATE TABLE IF NOT EXISTS sync_projection_read_selection (
    selector_id TINYINT PRIMARY KEY,
    version_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    contract_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    handoff_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    read_revision BIGINT NOT NULL,
    current_operation_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    updated_at DATETIME(6) NOT NULL,
    FOREIGN KEY(version_id) REFERENCES sync_projection_versions(version_id),
    FOREIGN KEY(handoff_id) REFERENCES sync_projection_handoffs(handoff_id),
    CHECK (selector_id=1 AND read_revision>0)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_projection_read_switches (
    operation_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    direction VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    from_version_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    to_version_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    from_contract_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    to_contract_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    ck_database_uuid CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    expected_read_revision BIGINT NOT NULL,
    freeze_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    freeze_revision BIGINT NOT NULL,
    stage VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    operation_revision BIGINT NOT NULL,
    marker_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    marker_issued_at DATETIME(6) NULL,
    proof JSON NULL,
    proof_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    validated_at DATETIME(6) NULL,
    last_error VARCHAR(1024) NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    cancelled_at DATETIME(6) NULL,
    FOREIGN KEY(from_version_id) REFERENCES sync_projection_versions(version_id),
    FOREIGN KEY(to_version_id) REFERENCES sync_projection_versions(version_id),
    UNIQUE KEY uk_read_switch_marker(marker_id),
    KEY idx_read_switch_stage(stage,operation_id),
    CHECK (direction IN ('PROMOTE','ROLLBACK')),
    CHECK (stage IN ('PLANNED','CANCELLED','COMMITTED','API_READY','RESUMED')),
    CHECK (expected_read_revision>0 AND freeze_revision>0 AND operation_revision>0),
    CHECK ((marker_id IS NULL AND marker_issued_at IS NULL) OR
           (marker_id IS NOT NULL AND marker_issued_at IS NOT NULL)),
    CHECK ((proof IS NULL AND proof_sha256 IS NULL AND validated_at IS NULL) OR
           (proof IS NOT NULL AND proof_sha256 IS NOT NULL AND validated_at IS NOT NULL))
) ENGINE=InnoDB;
