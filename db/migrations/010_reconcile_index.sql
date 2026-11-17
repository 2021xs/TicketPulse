-- M5-02 ordered all-organization recovery audit. Apply once to existing databases.
ALTER TABLE ticket_histories ADD INDEX idx_history_reconcile (organization_id,ticket_id,version);
