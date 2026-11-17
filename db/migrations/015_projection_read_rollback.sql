-- Explicit C3 installation. 011/012 Contracts and 013/014 bytes stay unchanged.
ALTER TABLE sync_projection_read_switches
    ADD COLUMN rollback_of VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ADD COLUMN superseded_by VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL;
CREATE TABLE IF NOT EXISTS sync_projection_legacy_boots (
    boot_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    component VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    group_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    identity JSON NOT NULL,
    identity_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    member_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    generation_id INT NOT NULL DEFAULT -1,
    state VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    heartbeat_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    KEY idx_legacy_component(component,group_id,heartbeat_at),
    CHECK(component IN ('es','ck')),
    CHECK(state IN ('STARTING','RUNNING','STOPPED'))
) ENGINE=InnoDB;
