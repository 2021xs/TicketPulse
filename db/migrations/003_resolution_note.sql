-- Add the latest resolution text without rewriting existing tickets or history JSON.
ALTER TABLE tickets
    ADD COLUMN resolution_note TEXT NULL AFTER description,
    ADD CONSTRAINT ck_tickets_resolution_note CHECK (resolution_note IS NULL OR CHAR_LENGTH(resolution_note) BETWEEN 1 AND 5000);
