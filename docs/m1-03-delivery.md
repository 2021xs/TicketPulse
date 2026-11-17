# M1-03 后端协作交付

本轮已完成同团队交接、主管跨团队转派、失效人员和团队恢复、主管人工关闭、工单评论及三个基础列表。真实工单 **TP-000000000049** 从 IT 服务台进入设备服务台，完成评论、重新认领、两次解决、重开和人工关闭，最终为已关闭、v13、两条评论和十三条连续历史。首次解决时间和两次说明均保留。

实现范围来自 PRD v0.2 §3.5、§4.3、FR-03/04/05/06/07，以及本轮用户明确冻结的异常重新入队规则。代码位于原 TicketPulse 分支，未暂存、提交、推送或创建远端。运行依赖仍只有 MySQL；登录会话和 Web 页面尚未开发。

下文 CODE 表示直接源码证据，RUNTIME 表示本次真实执行结果，INFER 表示下一步建议，UNKNOWN 表示未执行的边界。开发前保存了 43 个源码、文档和配置文件；集合哈希为 `0ed66b460e1c41c36fa0fd083864609315441ddbacba7ca4c5aa7c8e82b84ed9`，与 M1-02B 最终源码无差异。基线归档和逐文件哈希在 [本轮基线](../artifacts/m1-03/baseline-source-manifest.json)。

CODE：新增五个写接口及一个列表入口。写接口沿用正整数 ticket_id、expected_version，JSON 额外身份字段被拒绝，调用者来自服务端 Bearer。成功响应均为 200 和提交后的工单，评论额外返回 comment。完整字段和错误合同见 [API 文档](api.md)。

| 接口 | 额外输入 | 用途 |
| --- | --- | --- |
| POST /api/v1/tickets/transfer | assignee_id；reason 可选 | 同团队交接，或主管为无负责人/失效负责人指定合法人员 |
| POST /api/v1/tickets/transfer-team | team_id、必填 reason | 主管跨团队转派并重新入队 |
| POST /api/v1/tickets/management-reopen | team_id 可选、必填 reason | 主管人工重开，必要时在同事务换有效团队 |
| POST /api/v1/tickets/manual-close | 必填 reason | 主管对待确认工单人工关闭，无时长阈值 |
| POST /api/v1/tickets/comments | 必填 content | 保存评论、递增工单版本及更新时间、记录关联历史 |
| GET /api/v1/tickets | view、page、page_size | 我提交的、分配给我的、本团队待处理 |

原因最多 1,000 个 Unicode 字符，评论最多 5,000 个，均不能全部为空白、保留原文。列表默认 page=1、page_size=20，上限为 100 行和 10000 页。更新时间降序，主键降序打破同时间并列；total 和分页数据在同一授权快照读取，不返回越权计数。固定数据集分页稳定，持续变化时不提供跨页快照。

CODE：权限按当前账号、当前团队和当前负责人判断。主管/管理员具有组织范围权限，不能默认替代 Agent 开始处理或提交解决。

| 操作 | 有效创建人 | 当前团队 Agent | 组织 Supervisor/Admin |
| --- | --- | --- | --- |
| 详情、评论 | 本人工单 | 当前有效团队内 | 组织内 |
| 同团队转派 | 提单人关系不授予该权 | 仅有效当前负责人 | 待处理/处理中可指定有效 Agent |
| 跨团队转派 | 不可 | 不可 | 待处理/处理中，必填原因 |
| 开始、解决 | 提单人关系不授予该权 | 仅有效当前负责人及合法源状态 | 需交给有效 Agent |
| 普通重开、确认关闭 | 本人待确认工单 | 仅本人是创建人时 | 仅本人是创建人时 |
| 管理重开、人工关闭 | 不可 | 不可 | 仅待确认，必填原因 |
| 团队待处理列表 | Requester 无处理队列 | 全部有效成员团队 | 组织内有效团队队列 |

同团队转派保持状态，目标必须是当前有效团队的其他有效 Agent。无负责人的待处理工单只允许主管/管理员指定人员。旧负责人立即失去开始/解决权，但仍可凭团队成员关系查看和评论。跨团队转派清空负责人，处理中显式回到待处理，初始团队不变；旧团队普通成员失去详情、评论和队列权限，创建人及主管权限保留。

CODE：本轮修复 M1-02B 的失效负责人死角。下表所有重开成功分支都只增加一次版本和重开次数，保留首次解决时间和最近说明。

| 待确认工单的恢复条件 | 结果 | 审计动作 |
| --- | --- | --- |
| 提单人重开，团队/负责人均有效 | 处理中，原负责人保留 | reopened |
| 提单人重开，团队有效、负责人失效或为空 | 待处理，清空负责人 | reopened_requeued，保存系统原因 |
| 提单人重开，当前团队失效 | 数据不变，409 TEAM_UNAVAILABLE | 无成功历史，提示主管恢复 |
| 主管留原因重开，当前团队有效 | 负责人有效则处理中，否则待处理/空负责人 | management_reopened |
| 主管留原因重开到不同有效团队 | 待处理、新团队、空负责人 | management_reopened，记录原/新团队 |
| 提单人确认或主管留原因关闭 | 已关闭，设置关闭时间 | closed 或 manually_closed |

待确认仍不能直接转派，已关闭不能重开。主管对已失效团队的待处理/处理中工单可直接跨团队转派，对待确认使用管理重开接口。非法状态、无效目标、失权、旧版本和数据库失败均拒绝，不生成成功历史。授权先于版本检查：仍有权限的旧版本返回 VERSION_CONFLICT，已失权者返回权限错误。

CODE：调用链仍为 HTTP → Service → Store → MySQL。写动作共用 updateAction 的 READ COMMITTED 事务：锁定有效用户、FOR UPDATE 工单，校验当前/目标团队和成员，执行明确业务规则，再用 expected_version 条件 UPDATE 更新工单，插入同事务历史，Commit 后返回。转派、管理重开和评论只扩展本地回调，不引入通用工作流或权限框架。跨团队历史负责团队取生效后的新团队，before/after 保留原/新状态；先前历史的归属不改写。

评论独立保存作者、正文、版本和时间，成功同时递增工单版本/更新时间并新增 commented 历史，comment_id 指向正文记录。评论和历史/工单任一写入失败都会整体回滚。所有有查看权的人可评论全部状态，包括已关闭；评论不改变关闭状态、关闭时间或首次解决时间。M2 可从已提交工单版本、历史和评论表识别业务变更，M1 不实现传播或投影。

| 修改入口 | 内容 |
| --- | --- |
| internal/ticket/collaboration.go、list.go | 转派/管理用例、请求校验、明确状态规则、列表合同 |
| internal/ticket/model.go、service.go、actions.go、lifecycle.go、errors.go | 评论/历史字段、Repository 扩展、重新入队规则及错误 |
| internal/mysqlstore/actions.go、collaboration.go、history.go | 共用锁定事务、有效目标、权限、评论关联历史 |
| internal/mysqlstore/detail.go、list.go | 同快照详情/评论读取、SQL 授权列表及计数 |
| internal/httpapi/api.go、collaboration.go | 新接口、严格 JSON/查询参数、统一错误 |
| db/schema.sql、embed.go、migrations/004_collaboration.sql | 评论表、历史原因/关联、列表索引及迁移资产 |
| internal/ticket/collaboration_test.go、internal/mysqlstore/collaboration_integration_test.go | 输入、业务、权限、恢复、分页、HTTP、竞争、回滚和迁移验证 |
| lifecycle_test.go、lifecycle_integration_test.go | 只调整已冻结的新恢复行为，保留其他原回归 |
| scripts/smoke_collaboration.py、Makefile | 复用原 HTTP 工具，新增一条可重复协作演示 |
| README.md、AGENTS.md、docs/api.md、docs/design.md | 当前合同、状态机、迁移和下一步入口 |

[完整 Diff](../artifacts/m1-03/changes.patch) 从本轮基线归档生成；[逐文件哈希及代码量](../artifacts/m1-03/changes-manifest.json) 区分所有旧新文件。实现与测试净增 **1,864 行**：Go 业务 734、Go 测试 929、SQL 61、Python smoke 140，包含空行和注释，文档/构建配置另计。没有新依赖，创建指纹、路由、种子、旧 smoke 和原 MySQL 隔离工具保留。

RUNTIME：004 已应用到项目拥有的 MySQL 8.4.5 演示库。迁移前后逐列比对原有 4 张工单、16 条历史和 4 条幂等记录，所有旧值保持，新增评论表为空、旧历史原因/关联为 null。迁移必须在 003 已应用且 004 未应用时执行一次；DDL 按语句提交，部分失败后须核对结构，只继续未执行的语句。测试另外还原旧结构并执行真实迁移，旧历史 JSON 和请求键/hash 保留。结果见 [迁移证据](../artifacts/m1-03/migration-result.json)。

RUNTIME：make test、make integration、make vet、make build 和四个 smoke 均通过。集成命令含 `-race -tags=integration -count=1`，MySQL 包 50 个顶层测试通过，其中本轮新增 14 个；全包顶层共 59 个。测试库按场景独立创建、完成后删除，没有剩余随机测试库。

| 验收重点 | 真实结果与测试 |
| --- | --- |
| 同团队合法交接、主管分配/恢复失效负责人 | 旧负责人无开始/解决权；非负责人拒绝；TestSameTeamTransferAndAssignment |
| 跨团队重新入队及即时失权 | 新团队可认领，旧团队无详情/评论/队列权限；TestCrossTeamTransferCurrentAuthorization |
| 无效目标、非法状态、过期版本、伪造身份 | Store/HTTP 明确拒绝；TestCollaborationPermissionAndTargets、TestCollaborationHTTPErrorsAndRace |
| 停用人员提单人重开、失效团队主管恢复 | 实际 HTTP 成功重新入队，首次时间保持；TestRequeueAndManagementRecovery |
| 管理人工重开/关闭 | 有原因、有完整历史，普通用户无绕过权；TestManagementReopenValidOwner |
| 评论授权、最大长度、稳定顺序及关闭后评论 | 正文完整、每次一条关联历史；TestCommentsCurrentPermissionsAndSnapshots、TestMaximumCommentPersistenceAndClosedComments |
| 三列表及多团队关系、同时间分页和授权总数 | 实时反映状态和转派；TestListsCurrentStateAndMultipleMembership、TestListsStablePaginationAndAuthorizedTotal |
| 同版本竞争 | 转派/跨团队、转派/评论、管理重开/人工关闭只一个成功；Store 与 HTTP 均验证 |
| 历史、工单或评论写失败 | 真实 trigger 注入，无部分提交；HTTP 返回 INTERNAL_ERROR；TestCollaborationHistoryAndWriteRollback |
| 原有创建幂等、详情权限及 lifecycle | 前三轮全回归；旧工单 1、19、29、30 保持原状态 |

RUNTIME：工单 49 的完整演示如下。评论也增加版本，因此解决发生在 v10 和 v12。两条评论原文为“网络异常影响会议，请协助定位。”与“已更换交换机并验证连通性。”。

| 版本 | 操作者 | 动作 | 当前团队/负责人 | 状态 |
| --- | --- | --- | --- | --- |
| 1 | Alice 1 | 创建 | IT 10 / 空 | 待处理 |
| 2 | IT Agent 3 | 认领 | IT 10 / 3 | 待处理 |
| 3 | IT Agent 3 | 开始处理 | IT 10 / 3 | 处理中 |
| 4 | Alice 1 | 评论 | IT 10 / 3 | 处理中 |
| 5 | IT Agent 3 | 同团队交接给 11 | IT 10 / 11 | 处理中 |
| 6 | 主管 5 | 跨团队转派，原因“设备团队更换交换机” | 设备 20 / 空 | 待处理 |
| 7 | 设备 Agent 4 | 重新认领 | 设备 20 / 4 | 待处理 |
| 8 | 设备 Agent 4 | 开始处理 | 设备 20 / 4 | 处理中 |
| 9 | 设备 Agent 4 | 评论 | 设备 20 / 4 | 处理中 |
| 10 | 设备 Agent 4 | 首次解决 | 设备 20 / 4 | 待确认 |
| 11 | Alice 1 | 重开 | 设备 20 / 4 | 处理中 |
| 12 | 设备 Agent 4 | 再次解决 | 设备 20 / 4 | 待确认 |
| 13 | 主管 5 | 人工关闭，原因“主管与员工核验后人工确认关闭” | 设备 20 / 4 | 已关闭 |

HTTP 每步请求、评论正文、列表快照及失权拒绝在 [协作演示](../artifacts/m1-03/smoke-result.json)。[独立 SQL 回读](../artifacts/m1-03/database-result.json) 核对完整字段、前后历史链、两次解决说明、评论关联及初始/当前团队；时间按 UTC 微秒值比较，HTTP 允许省略小数末尾零。统计为一张工单、十三条历史、两条评论和一条创建幂等记录。make smoke-collaboration 重复执行不增加工单或历史。

UNKNOWN：没有扩展到 Commit 网络中断、断电恢复、专门锁超时/死锁注入、跨平台或故障压测；也没有新增动作 request_id 原结果重放。详情一次返回该工单全部评论/历史，尚未做长时间线分页。基础列表不是高级搜索，权限在请求快照核验，持续变更不承诺跨页快照。

INFER：M1 余下最短入口是预置账号登录/退出、有限期会话、当前用户与角色信息，以及团队/有效 Agent 选择数据，再接轻量 Web 工作台。三个固定列表、创建详情、评论和动作 API 已可复用；页面需保留创建 request_id 和读取时版本，冲突后刷新、评论失败保留输入。账号管理界面、复杂权限引擎、自动关闭、SLA、M2/M3 都不属于本轮。
