-- Apply exactly once to M1-02B (003_resolution_note.sql already applied).
-- MySQL DDL commits per statement: inspect information_schema before resuming a partially applied migration.
CREATE TABLE ticket_comments (
    id BIGINT NOT NULL AUTO_INCREMENT,
    organization_id BIGINT NOT NULL,
    ticket_id BIGINT NOT NULL,
    author_id BIGINT NOT NULL,
    content TEXT NOT NULL,
    version BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_comments_org_ticket_id (organization_id, ticket_id, id),
    UNIQUE KEY uk_comments_ticket_version (ticket_id, version),
    KEY idx_comments_ticket_time (organization_id, ticket_id, created_at, id),
    CONSTRAINT fk_comments_ticket FOREIGN KEY (organization_id, ticket_id) REFERENCES tickets (organization_id, id),
    CONSTRAINT fk_comments_author FOREIGN KEY (organization_id, author_id) REFERENCES users (organization_id, id),
    CONSTRAINT ck_comments_content CHECK (CHAR_LENGTH(content) BETWEEN 1 AND 5000),
    CONSTRAINT ck_comments_version CHECK (version >= 2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

ALTER TABLE ticket_histories
    ADD COLUMN reason TEXT NULL,
    ADD COLUMN comment_id BIGINT NULL,
    ADD CONSTRAINT fk_history_comment FOREIGN KEY (organization_id, ticket_id, comment_id) REFERENCES ticket_comments (organization_id, ticket_id, id),
    ADD CONSTRAINT ck_history_reason CHECK (reason IS NULL OR CHAR_LENGTH(reason) BETWEEN 1 AND 1000),
    ADD CONSTRAINT ck_history_required_reason CHECK (action NOT IN ('team_transferred', 'management_reopened', 'manually_closed', 'reopened_requeued') OR reason IS NOT NULL),
    ADD CONSTRAINT ck_history_comment CHECK ((action = 'commented' AND comment_id IS NOT NULL) OR (action <> 'commented' AND comment_id IS NULL));

ALTER TABLE tickets
    DROP INDEX idx_tickets_creator,
    DROP INDEX idx_tickets_team_status,
    ADD INDEX idx_tickets_creator (organization_id, creator_id, updated_at DESC, id DESC),
    ADD INDEX idx_tickets_team_status (organization_id, current_team_id, status, updated_at DESC, id DESC),
    ADD INDEX idx_tickets_assignee_updated (organization_id, assignee_id, updated_at DESC, id DESC),
    ADD INDEX idx_tickets_status_updated (organization_id, status, updated_at DESC, id DESC);
