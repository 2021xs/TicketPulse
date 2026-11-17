-- Consolidated new-database baseline through M1-04; existing databases use explicit SQL migrations.
-- MySQL 8.0.16+ (InnoDB, enforced CHECK constraints); verified against MySQL 8.4.5.
CREATE TABLE users (
    id BIGINT NOT NULL,
    organization_id BIGINT NOT NULL,
    name VARCHAR(100) NOT NULL,
    role VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    auth_token_hash BINARY(32) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_users_org_id (organization_id, id),
    UNIQUE KEY uk_users_auth_token (auth_token_hash),
    CONSTRAINT ck_users_role CHECK (role IN ('requester', 'agent', 'supervisor', 'admin')),
    CONSTRAINT ck_users_active CHECK (active IN (0, 1)),
    CONSTRAINT ck_users_org CHECK (organization_id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE login_accounts (
    user_id BIGINT NOT NULL,
    username VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    password_hash VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (user_id),
    UNIQUE KEY uk_login_username (username),
    CONSTRAINT fk_login_user FOREIGN KEY (user_id) REFERENCES users (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE user_sessions (
    token_hash BINARY(32) NOT NULL,
    user_id BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    revoked_at DATETIME(6) NULL,
    PRIMARY KEY (token_hash),
    KEY idx_sessions_user_expiry (user_id, expires_at),
    CONSTRAINT fk_session_user FOREIGN KEY (user_id) REFERENCES users (id),
    CONSTRAINT ck_session_expiry CHECK (expires_at > created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE teams (
    id BIGINT NOT NULL,
    organization_id BIGINT NOT NULL,
    name VARCHAR(100) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_teams_org_id (organization_id, id),
    CONSTRAINT ck_teams_active CHECK (active IN (0, 1)),
    CONSTRAINT ck_teams_org CHECK (organization_id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE team_members (
    organization_id BIGINT NOT NULL,
    team_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (team_id, user_id),
    KEY idx_members_org_user (organization_id, user_id, active, team_id),
    CONSTRAINT fk_members_team FOREIGN KEY (organization_id, team_id) REFERENCES teams (organization_id, id),
    CONSTRAINT fk_members_user FOREIGN KEY (organization_id, user_id) REFERENCES users (organization_id, id),
    CONSTRAINT ck_members_active CHECK (active IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE tickets (
    id BIGINT NOT NULL,
    organization_id BIGINT NOT NULL,
    ticket_no VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    title VARCHAR(120) NOT NULL,
    description TEXT NOT NULL,
    resolution_note TEXT NULL,
    category VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    priority VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    status VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    creator_id BIGINT NOT NULL,
    assignee_id BIGINT NULL,
    initial_team_id BIGINT NOT NULL,
    current_team_id BIGINT NOT NULL,
    version BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    first_resolved_at DATETIME(6) NULL,
    closed_at DATETIME(6) NULL,
    reopen_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tickets_no (ticket_no),
    UNIQUE KEY uk_tickets_org_id (organization_id, id),
    KEY idx_tickets_creator (organization_id, creator_id, updated_at DESC, id DESC),
    KEY idx_tickets_team_status (organization_id, current_team_id, status, updated_at DESC, id DESC),
    KEY idx_tickets_assignee_updated (organization_id, assignee_id, updated_at DESC, id DESC),
    KEY idx_tickets_status_updated (organization_id, status, updated_at DESC, id DESC),
    CONSTRAINT fk_tickets_creator FOREIGN KEY (organization_id, creator_id) REFERENCES users (organization_id, id),
    CONSTRAINT fk_tickets_assignee FOREIGN KEY (organization_id, assignee_id) REFERENCES users (organization_id, id),
    CONSTRAINT fk_tickets_initial_team FOREIGN KEY (organization_id, initial_team_id) REFERENCES teams (organization_id, id),
    CONSTRAINT fk_tickets_current_team FOREIGN KEY (organization_id, current_team_id) REFERENCES teams (organization_id, id),
    CONSTRAINT ck_tickets_version CHECK (version >= 1),
    CONSTRAINT ck_tickets_reopen CHECK (reopen_count >= 0),
    CONSTRAINT ck_tickets_title CHECK (CHAR_LENGTH(title) BETWEEN 5 AND 120),
    CONSTRAINT ck_tickets_description CHECK (CHAR_LENGTH(description) BETWEEN 1 AND 5000),
    CONSTRAINT ck_tickets_resolution_note CHECK (resolution_note IS NULL OR CHAR_LENGTH(resolution_note) BETWEEN 1 AND 5000),
    CONSTRAINT ck_tickets_category CHECK (category IN ('network', 'account', 'device', 'software', 'other')),
    CONSTRAINT ck_tickets_priority CHECK (priority IN ('urgent', 'high', 'medium', 'low')),
    CONSTRAINT ck_tickets_status CHECK (status IN ('pending', 'in_progress', 'awaiting_confirmation', 'closed'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

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

CREATE TABLE ticket_histories (
    id BIGINT NOT NULL AUTO_INCREMENT,
    organization_id BIGINT NOT NULL,
    ticket_id BIGINT NOT NULL,
    action VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    operator_id BIGINT NOT NULL,
    version BIGINT NOT NULL,
    before_state JSON NULL,
    after_state JSON NOT NULL,
    responsible_team_id BIGINT NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    reason TEXT NULL,
    comment_id BIGINT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_history_ticket_version (ticket_id, version),
    KEY idx_history_reconcile (organization_id, ticket_id, version),
    KEY idx_history_operator (organization_id, operator_id),
    KEY idx_history_team (organization_id, responsible_team_id),
    CONSTRAINT fk_history_ticket FOREIGN KEY (organization_id, ticket_id) REFERENCES tickets (organization_id, id),
    CONSTRAINT fk_history_operator FOREIGN KEY (organization_id, operator_id) REFERENCES users (organization_id, id),
    CONSTRAINT fk_history_team FOREIGN KEY (organization_id, responsible_team_id) REFERENCES teams (organization_id, id),
    CONSTRAINT fk_history_comment FOREIGN KEY (organization_id, ticket_id, comment_id) REFERENCES ticket_comments (organization_id, ticket_id, id),
    CONSTRAINT ck_history_reason CHECK (reason IS NULL OR CHAR_LENGTH(reason) BETWEEN 1 AND 1000),
    CONSTRAINT ck_history_required_reason CHECK (action NOT IN ('team_transferred', 'management_reopened', 'manually_closed', 'reopened_requeued') OR reason IS NOT NULL),
    CONSTRAINT ck_history_comment CHECK ((action = 'commented' AND comment_id IS NOT NULL) OR (action <> 'commented' AND comment_id IS NULL)),
    CONSTRAINT ck_history_version CHECK (version >= 1),
    CONSTRAINT ck_history_creation CHECK (action <> 'created' OR (version = 1 AND before_state IS NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

-- This AUTO_INCREMENT supplies the unique ticket id/number inside the create transaction.
-- A reservation with NULL ticket_id is never committed by the application.
CREATE TABLE ticket_create_requests (
    id BIGINT NOT NULL AUTO_INCREMENT,
    organization_id BIGINT NOT NULL,
    creator_id BIGINT NOT NULL,
    request_id VARBINARY(64) NOT NULL,
    payload_hash BINARY(32) NOT NULL,
    ticket_id BIGINT NULL,
    created_at DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_create_user_request (creator_id, request_id),
    UNIQUE KEY uk_create_ticket (ticket_id),
    KEY idx_create_org_user (organization_id, creator_id),
    KEY idx_create_org_ticket (organization_id, ticket_id),
    CONSTRAINT fk_create_user FOREIGN KEY (organization_id, creator_id) REFERENCES users (organization_id, id),
    CONSTRAINT fk_create_ticket FOREIGN KEY (organization_id, ticket_id) REFERENCES tickets (organization_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
