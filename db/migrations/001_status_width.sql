-- Preserve existing tickets while accommodating PRD awaiting_confirmation (21 ASCII characters).
ALTER TABLE tickets MODIFY COLUMN status VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL;
