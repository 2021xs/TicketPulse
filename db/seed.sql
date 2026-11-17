-- Local fixtures only. Never overwrite existing accounts or business rows.
INSERT INTO users (id, organization_id, name, role, active, auth_token_hash, created_at, updated_at) VALUES
(1, 1, 'Alice 提单人', 'requester', TRUE, UNHEX(SHA2('local-alice',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(2, 1, 'Bob 提单人', 'requester', TRUE, UNHEX(SHA2('local-bob',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(3, 1, 'IT 受理人', 'agent', TRUE, UNHEX(SHA2('local-it-agent',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(4, 1, '设备受理人', 'agent', TRUE, UNHEX(SHA2('local-device-agent',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(5, 1, '运营主管', 'supervisor', TRUE, UNHEX(SHA2('local-supervisor',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(6, 1, '组织管理员', 'admin', TRUE, UNHEX(SHA2('local-admin',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(7, 1, '已退出 IT 团队', 'agent', TRUE, UNHEX(SHA2('local-ex-member',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(8, 1, '停用账号', 'agent', FALSE, UNHEX(SHA2('local-disabled',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(9, 2, '其他组织管理员', 'admin', TRUE, UNHEX(SHA2('local-other-admin',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(10, 1, '默认团队受理人', 'agent', TRUE, UNHEX(SHA2('local-default-agent',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(11, 1, 'IT 第二工程师', 'agent', TRUE, UNHEX(SHA2('local-it-agent-2',256)), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6));

INSERT INTO teams (id, organization_id, name, active, created_at, updated_at) VALUES
(10, 1, 'IT 服务台', TRUE, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(20, 1, '设备服务台', TRUE, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(90, 1, '综合服务台', TRUE, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(99, 1, '停用团队', FALSE, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6)),
(200, 2, '其他组织团队', TRUE, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6));

INSERT INTO team_members (organization_id, team_id, user_id, active, created_at) VALUES
(1, 10, 3, TRUE, UTC_TIMESTAMP(6)),
(1, 20, 4, TRUE, UTC_TIMESTAMP(6)),
(1, 10, 7, FALSE, UTC_TIMESTAMP(6)),
(1, 10, 8, TRUE, UTC_TIMESTAMP(6)),
(1, 90, 10, TRUE, UTC_TIMESTAMP(6)),
(2, 200, 9, TRUE, UTC_TIMESTAMP(6)),
(1, 10, 11, TRUE, UTC_TIMESTAMP(6));
