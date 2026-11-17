-- M4-03A additive control metadata. No projection or business row is changed.
CREATE TABLE IF NOT EXISTS sync_business_write_gate (
    gate_id TINYINT PRIMARY KEY,
    state VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    revision BIGINT NOT NULL,
    freeze_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    reason VARCHAR(500) NOT NULL,
    frozen_at DATETIME(6) NULL,
    updated_at DATETIME(6) NOT NULL,
    CHECK (gate_id=1),
    CHECK (state IN ('OPEN','FROZEN')),
    CHECK (revision>=0),
    CHECK ((state='OPEN' AND freeze_id IS NULL AND frozen_at IS NULL) OR
           (state='FROZEN' AND freeze_id IS NOT NULL AND frozen_at IS NOT NULL))
) ENGINE=InnoDB;
INSERT IGNORE INTO sync_business_write_gate(gate_id,state,revision,reason,updated_at)
VALUES(1,'OPEN',0,'',UTC_TIMESTAMP(6));
CREATE TABLE IF NOT EXISTS sync_projection_versions (
    version_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    phase VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    contract JSON NOT NULL,
    validation_report JSON NULL,
    report_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    freeze_revision BIGINT NULL,
    checked_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    active_slot TINYINT GENERATED ALWAYS AS (IF(phase='ACTIVE',1,NULL)) STORED,
    UNIQUE KEY uk_projection_active(active_slot),
    CHECK (phase IN ('ACTIVE','PREPARED'))
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_projection_progress_bindings (
    component VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    version_id VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    binding JSON NOT NULL,
    created_at DATETIME(6) NOT NULL,
    FOREIGN KEY(version_id) REFERENCES sync_projection_versions(version_id),
    CHECK (component IN ('es','ck'))
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS sync_projection_control_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    operation VARCHAR(32) NOT NULL,
    reference_id VARCHAR(32) NULL,
    details JSON NOT NULL,
    occurred_at DATETIME(6) NOT NULL
) ENGINE=InnoDB;
