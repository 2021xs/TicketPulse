# M1-02A 工单认领与开始处理交付

本轮已完成两个业务动作并通过真实 MySQL 与 HTTP 验证。两位有效 IT 工程师竞争认领同一工单时，一人成功、另一人收到版本冲突；成功者随后开始处理。演示工单为 **TP-000000000019**，最终负责人为 3，状态为处理中，version=3，历史按版本返回创建、认领、开始处理三条记录。

## 工作区、基线与范围

工程沿用独立 TicketPulse，具体目录见 artifacts/m1-02a/workspace-result.json。分支保持 feat/m1-01-ticket-loop；仍没有基线提交、没有 remote，所有源码未暂存。本轮没有自动提交或推送。

动手前保存了 29 个源码、配置和文档文件的路径、SHA-256 与源码归档：artifacts/m1-02a/baseline-source-manifest.json、baseline-source.tar.gz。集合哈希为 `3d4bdc9e9c0e45f2a3fbc0180e37a1541352b31f821857873d082a1dec135599`，每个归档文件均与清单核对。旧验收材料保存在该目录的 m1-01-evidence/，本轮开始时的单元、真实数据库/race 和 HTTP smoke 结果在 precheck-result.json。

认领与开始处理没有改变表结构、路由配置或依赖。只在 seed.sql 和显式迁移 002_second_it_agent.sql 中增加第二位合成 IT 工程师（id=11、Bearer local-it-agent-2），已有测试账号和工单保留。未开发转派、解决、重开、关闭、评论、前端或中间件。

## 实际文件变更与核心 Diff

完整可审阅 Diff 在 artifacts/m1-02a/changes.patch，逐文件新旧哈希和增删行数在 changes-manifest.json。它来自本轮保存的源码基线，无需 Git 提交来识别变更。

| 文件 | 实际改动 |
| --- | --- |
| internal/ticket/actions.go（新增） | 两个 Service 用例、请求校验、Ticket.Claim/Start 规则 |
| internal/ticket/model.go、service.go、errors.go | 新动作常量、Repository 方法与动作错误码 |
| internal/mysqlstore/actions.go（新增） | 两动作共用的本地事务，权限核验、行锁、期望版本更新、提交结果 |
| internal/mysqlstore/history.go（新增） | 从创建流程抽取事务历史写入，支持 before/after |
| internal/mysqlstore/store.go、detail.go、create.go | 工单读取可选 FOR UPDATE；抽取有效成员查询；创建和详情消费帮助函数 |
| internal/httpapi/api.go | 注册两个接口，复用 Bearer、JSON 与错误处理，区分动作错误文案 |
| internal/ticket/actions_test.go、internal/mysqlstore/actions_integration_test.go（新增） | 单元规则、真实数据库并发/权限/回滚、HTTP 与响应丢失测试 |
| db/seed.sql、db/migrations/002_second_it_agent.sql | 第二个 IT 工程师，用于真实竞争 |
| scripts/smoke_actions.py（新增）、Makefile | 独立动作 smoke，保存竞争与详情证据，增加 make smoke-actions |
| AGENTS.md、README.md、docs/api.md、docs/design.md、本报告 | 更新当前范围、接口、事务、重试及验收入口 |

核心写入顺序是“锁定工单→当前成员与业务规则→带版本更新→历史→Commit”。更新列限定为负责人、状态、版本和更新时间：

~~~sql
UPDATE tickets
SET assignee_id = ?, status = ?, version = ?, updated_at = ?
WHERE organization_id = ? AND id = ? AND version = ?;
~~~

WHERE 的最后参数使用客户端读取时的 expected_version；新 version 只由服务端递增。更新与 insertHistory 使用同一个 sql.Tx，失败立即回滚，成功返回在 Commit 之后。

## HTTP 协议与调用链

| 接口 | JSON 输入 | 成功行为 |
| --- | --- | --- |
| POST /api/v1/tickets/claim | ticket_id、expected_version，均为正整数 | 200；负责人为当前 Agent，状态仍待处理，version+1 |
| POST /api/v1/tickets/start | ticket_id、expected_version，均为正整数 | 200；当前负责人保持，状态变处理中，version+1 |

请求要求 application/json 和有效 Bearer，成功体为 `{"ticket": {...}}`。不能传入当前用户、角色、团队或指定负责人。错误体沿用 `{"error":{"code":"...","message":"..."}}`，不返回工单内容、内部 SQL 或凭据。完整字段规则见 docs/api.md。

真实调用链为 HTTP.claim/start→ticketAction→Service.Claim/Start→Store.Claim/Start→updateAction→MySQL。Service 校验输入；Store 使用服务器解析的用户 ID 重新加载权威身份并核验当前团队。业务规则在 Ticket.Claim/Start，状态历史在 mysqlstore/history.go。

## 并发、权限与事务

动作使用 READ COMMITTED 事务，锁顺序为有效用户 FOR SHARE、同组织工单 FOR UPDATE、当前团队和成员联表 FOR SHARE。工单行锁串行化同一工单修改；共享锁保证本次操作期间账号、角色、团队和成员有效性不能被并发改变。版本条件 UPDATE 再检查恰好一行更新。

认领要求有效 Agent、同组织当前有效团队成员、待处理、无负责人及版本匹配。成功只设置负责人，不开始处理。开始处理还要求当前用户等于 assignee_id，其账号、Agent 角色和当前团队成员关系仍有效；普通同事、Requester、Supervisor、Admin 没有默认开始处理权。

认领权限通过后先检查版本，再检查状态和是否已认领。开始处理先检查负责人，再检查版本和状态。错误分别为 401 UNAUTHENTICATED、403 ACTION_FORBIDDEN、404 TICKET_NOT_FOUND、409 VERSION_CONFLICT/ALREADY_CLAIMED/INVALID_STATE；数据库写入失败返回 500 INTERNAL_ERROR，未确认提交返回 503 ACTION_RESULT_UNKNOWN。

版本递增在 Ticket.Claim 与 Ticket.Start；updated_at 使用 UTC 微秒并向前更新。insertHistory 保存之前版本的完整业务状态、之后版本的状态、动作、操作者、操作时当前团队和同一更新时间。历史插入失败时负责人、状态、版本和更新时间都回滚，失败方不留下成功历史。

本轮动作重试合同按 expected_version：成功提交后响应丢失，沿用旧版本重试返回 VERSION_CONFLICT，不会再更新或再写历史。客户端先读取详情确认结果，不能自动换成新版本并继续提交。没有动作 request_id 重放记录；是否增加统一动作幂等仍留后续评审。

## 测试与真实执行结果

以下要求全部通过真实 MySQL 8.4.5 隔离测试。每个场景使用随机新数据库，结束后仅删除它自己的测试库，原演示库继续保留。

| 要求 | 实际验证 |
| --- | --- |
| 正常认领 1→2、状态不变 | TestClaimStartTimeline |
| 正常开始处理 2→3 | TestClaimStartTimeline |
| 多工程师并发只一人成功 | TestConcurrentEngineersClaim：20 并发，1 成功、19 VERSION_CONFLICT |
| 重复认领不追加历史 | TestActionRetriesNeverDuplicateHistory：旧版本冲突，当前版本 ALREADY_CLAIMED |
| 旧版本不能覆盖新状态 | 认领与开始处理旧版本均拒绝，工单/历史回读未变化 |
| 非团队、失效人员/成员不能操作 | TestActionPermissions 的两动作权限矩阵，含账号停用、移除成员、停用团队、角色变化 |
| 同团队非负责人不能开始处理 | TestStartRequiresCurrentOwner 与 HTTP 测试 |
| 非法状态不能操作 | TestInvalidActionStates：处理中、待确认、已关闭 |
| 历史插入失败整体回滚 | TestActionHistoryFailureRollsBackUpdate：两动作均用真实 MySQL trigger 注入失败 |
| 详情返回 1/2/3 连续历史 | 对比三版 before/after、负责人、状态、团队、操作者和时间 |
| M1-01 合同继续通过 | 原有 integration_test.go 与 service_test.go 内容哈希不变，全量执行通过 |

额外验证包括同一负责人十次并发开始处理仅一次成功、伪造用户/角色/团队字段被拒绝、动作 UPDATE 失败返回 500 且回滚、认领和开始处理提交后丢失 HTTP 响应再重试返回 409，以及创建幂等重试在动作后仍返回同一工单的当前版本。

真实运行服务的 smoke 以工程师 3 与 11 同时提交 expected_version=1，HTTP 结果为 200/409。胜者 3 使用 expected_version=2 开始处理，200 返回 version=3。详情返回三条历史，旧版本认领与开始处理重试 409，非负责人开始处理 403。证据在 artifacts/m1-02a/smoke-result.json；M1-01 的原 smoke 工单 1 仍为 version=1。

| 命令 | 结果与范围 |
| --- | --- |
| make test | 标准 Go 单元测试通过；不由此命令声称执行数据库测试 |
| make integration | 全量真实 MySQL 集成与 `-race -tags=integration -count=1` 通过 |
| make vet | 静态检查通过 |
| make build | 独立服务编译通过 |
| make smoke | 原创建/详情/幂等/权限 HTTP 回归通过 |
| make smoke-actions | 创建→并发认领→开始处理→详情真实 HTTP 通过 |

最终日志、执行元信息和验证摘要保存在 artifacts/m1-02a/final-integration.log、final-integration-result.json、validation-result.json。未执行断电持久化、MySQL Commit 阶段网络故障、锁超时/死锁专门故障注入、其他平台/MySQL 版本、容量压测或后续业务动作。当前错误与重试语义已声明，但这些未覆盖边界不作为本轮已验证事实。

## 代码增量与后续复用

相对 M1-01 基线，Go 业务代码净增 **260 行**，Go 测试净增 **718 行**，SQL 净增 **9 行**，Python smoke 净增 **123 行**，合计实现与测试净增 **1,110 行**。按物理行计算，包含空行与注释；文档、配置及原始结果另计，详见 artifacts/m1-02a/code-size.json。go.mod/go.sum 未改变，未新增依赖。

可继续复用 ActionRequest 输入、当前有效成员查询、行锁与版本检查、事务 history before/after 写入、错误映射、隔离测试夹具。转派需要另定义目标团队/人员权限与允许状态；解决、重开和关闭需要各自的业务规则、数据字段及操作者资格。不能直接用当前 updateAction 的 Agent 限制处理主管转派或提单人关闭，应在对应迭代明确新动作授权。

没有发现必须调整 M1-01 数据模型的证据，因此没有结构重构。此次只抽取已有历史与成员 SQL 帮助函数以避免三个用例重复实现，保持详情快照、创建幂等和历史合同；原有单元/集成测试及真实 smoke 已验证这些边界。
