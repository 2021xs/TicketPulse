# TicketPulse 工程约定

产品依据为 PRD v0.2（2026-11-03）及 M1-03 用户冻结的异常恢复规则。M1-01 创建/详情，M1-02A 认领/处理，M1-02B 解决/重开/关闭；M1-03 转派、主管恢复/人工关闭、评论和 MySQL 三列表；M1-04 密码登录、有限期 Cookie 会话、候选数据和 Web MVP。新迭代先读 README.md、docs/api.md、docs/design.md 及目标轮交付文档。

HTTP 协议在 internal/httpapi，规则与用例在 internal/ticket，MySQL 在 internal/mysqlstore，Web 在 web/ 并由 Go 内嵌同源提供。Cookie 写请求校验 Origin 和 X-CSRF-Token；密码用 bcrypt，随机会话仅存 SHA-256，禁止记录原凭据。业务状态、版本和对应历史必须放在同一个事务。当前用户与角色从服务器凭据和数据库取得，不接受客户端身份字段。

db/schema.sql 是新库基线。已有库后续结构变更使用显式迁移，不重新初始化已有数据。测试只连接本项目隔离 MySQL，创建自己的随机测试库；不连接现有业务实例。脚本拒绝覆盖非本项目数据。

常用命令：make test、make integration、make vet、make build、make smoke、make smoke-actions、make smoke-lifecycle、make smoke-collaboration、make smoke-session、make browser。动作及评论均使用 ticket_id、expected_version；旧版本重试返回冲突，不返回原结果。工单锁、当前人员/团队校验、版本更新、评论和历史属于同一事务。重开保留有效负责人；负责人失效且团队有效则清空负责人、回待处理并记录 reopened_requeued；团队失效返回 TEAM_UNAVAILABLE，由主管带原因 management-reopen 到有效团队。first_resolved_at 只写首次，reopen_count 每次重开加一，说明保留各次快照。评论增加工单版本与更新时间并写关联历史，列表及详情用请求内只读快照，不用旧团队授权。Go 测试采用标准 testing，运行 Go 依赖为 MySQL 驱动与 bcrypt，Playwright 仅验证浏览器。路径相对项目根目录，运行路径由脚本自身推导。

修改前核对当前源码与相关验收记录。公开文档只记录本项目行为和公开依赖，不包含组织内部标识、路径、链接或凭据。未经用户明确要求，不提交、不推送、不创建远端仓库。

M2-01：internal/syncpipe 捕获已提交 ROW/FULL Binlog，JetStream 发布持久确认后保存 MySQL 检查点，ES 严格 external version，成功/隔离持久化后才 AckSync。评论触发工单版本投影，正文不全文索引。internal/search 与 mysqlstore/search.go 批量执行当前 MySQL 授权，不返回原始 ES 计数或字段；游标加密绑定用户/查询，预算耗尽失败。006_search_sync.sql 是同步控制表唯一 DDL；运行 Go 依赖增加公开 go-mysql 与 NATS 客户端。make search-setup/sync/run-search/search-status/integration-search/smoke-search/browser-search；原生服务数据在忽略目录，测试只清理自己随机命名的数据库/流/索引。

M2-02 默认 Kafka：acks=all、幂等生产者、有限重试；008_kafka_sync.sql 保存独立捕获检查点、Topic ID、分区/Offset 失败记录及水位。旧 CDC 检查点使用 make kafka-migrate 接续，禁止直接从日志头切换。关闭自动 Offset 提交，按分区顺序在下游成功或持久隔离后提交，暂时失败仅暂停失败分区；组内可多实例，捕获保持单实例锁。NATS 已退出 Go 运行依赖，旧控制数据与 M2-01 证据保留。M3：消息含当前工单与不可变 History，CK 独立组用两张 MergeTree 表保存事实与状态；SQL 按动作版本去重/取首次、按最大版本取最新，正确性不依赖 merge。十工单 Gold 在 tests/analytics/prd_gold.json，禁止正式 SQL/计算函数生成预期。事件团队来自历史快照，当前团队仅用于积压；看板日期 Asia/Shanghai 左闭右开，普通用户不可访问统计。每分区 Kafka Offset 才是进度，不能将总和当连续水位。新空间 schema+006+007+008；已有数据只增迁移。make integration-analytics/smoke-analytics/browser-analytics。
