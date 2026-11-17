-- Add a second synthetic IT engineer to an existing owned local database; never replace accounts.
START TRANSACTION;
INSERT INTO users (id, organization_id, name, role, active, auth_token_hash, created_at, updated_at)
VALUES (11, 1, 'IT 第二工程师', 'agent', TRUE, UNHEX(SHA2('local-it-agent-2',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6));
INSERT INTO team_members (organization_id, team_id, user_id, active, created_at)
VALUES (1, 10, 11, TRUE, UTC_TIMESTAMP(6));
COMMIT;
