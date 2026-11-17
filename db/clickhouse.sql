-- Each database has its own owned namespace. Tables are append-only, no merge required for correctness.
CREATE TABLE IF NOT EXISTS ticket_events (
    organization_id UInt64,
    ticket_id UInt64,
    version UInt64,
    business_event_id String,
    transport_event_id String,
    action LowCardinality(String),
    occurred_at DateTime64(6,'UTC'),
    event_team_id UInt64,
    initial_team_id UInt64,
    before_team_id Nullable(UInt64),
    before_status LowCardinality(String),
    after_status LowCardinality(String),
    assignee_id Nullable(UInt64),
    created_at DateTime64(6,'UTC'),
    first_resolved_at Nullable(DateTime64(6,'UTC')),
    ingested_at DateTime64(6,'UTC') DEFAULT now64(6)
) ENGINE=MergeTree ORDER BY (organization_id,ticket_id,version);

CREATE TABLE IF NOT EXISTS ticket_states (
    organization_id UInt64,
    ticket_id UInt64,
    version UInt64,
    complete UInt8,
    current_team_id UInt64,
    assignee_id Nullable(UInt64),
    status LowCardinality(String),
    initial_team_id UInt64,
    created_at DateTime64(6,'UTC'),
    updated_at DateTime64(6,'UTC'),
    first_resolved_at Nullable(DateTime64(6,'UTC')),
    ingested_at DateTime64(6,'UTC') DEFAULT now64(6)
) ENGINE=MergeTree ORDER BY (organization_id,ticket_id,version);
