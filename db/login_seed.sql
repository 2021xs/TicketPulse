-- Public local demonstration credentials only; never overwrite an existing password/account.
-- Shared demo password: TicketPulse!2026 (bcrypt cost 12, plaintext is not stored in the database).
INSERT INTO login_accounts (user_id,username,password_hash)
SELECT u.id,n.username,'$2a$12$IF8KNCJBAzl43xgBieaAmulBVmSREoNFiTzlPTWPBL9VM4A2ydk5.'
FROM users u JOIN (
    SELECT 1 AS user_id,'alice' AS username UNION ALL
    SELECT 2,'bob' UNION ALL
    SELECT 3,'it-agent' UNION ALL
    SELECT 4,'device-agent' UNION ALL
    SELECT 5,'supervisor' UNION ALL
    SELECT 6,'admin' UNION ALL
    SELECT 7,'ex-member' UNION ALL
    SELECT 8,'disabled' UNION ALL
    SELECT 9,'other-admin' UNION ALL
    SELECT 10,'default-agent' UNION ALL
    SELECT 11,'it-agent-2'
) n ON n.user_id=u.id
WHERE NOT EXISTS (SELECT 1 FROM login_accounts a WHERE a.user_id=u.id OR a.username=n.username);
