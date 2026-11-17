# M1-02B 解决、重开与关闭交付

本轮已让真实工单完成创建、认领、开始处理、首次解决、重开、再次解决和确认关闭。演示工单 **TP-000000000030** 的最终状态为已关闭、version=7、负责人 3、reopen_count=1；首次解决时间等于版本 4 的操作时间，版本 6 再次解决未覆盖它。详情返回最近解决说明，历史保留两次说明原文和七版连续状态。

另一个工单 **TP-000000000029** 完成不重开的路径：处理中→待确认→已关闭，最终 version=5、reopen_count=0。旧演示工单 1 与 19 保留原状态；M1-01、M1-02A 全部回归和运行服务 smoke 均通过。

## 工作区与基线

沿用独立 TicketPulse 工程，绝对目录与 Git 状态见 artifacts/m1-02b/workspace-result.json。分支仍为 feat/m1-01-ticket-loop；没有 remote，没有提交或暂存。本轮没有接入 RPC、Redis、MQ、ES、CK 或前端。

修改前保存 37 个源码、配置和文档文件的路径、大小、行数与 SHA-256，并保存源码归档。基线集合哈希为 `9ea187df8819c1c138b923b4351c2c3fe4ba6a093704a68986870d6e9c226df3`，文件为 artifacts/m1-02b/baseline-source-manifest.json 和 baseline-source.tar.gz。归档与逐文件哈希均已核对，旧测试证据在 previous-evidence/，修改前的测试结果在 precheck-result.json。

## 新接口与输入输出

三个接口均要求有效 Bearer 和 application/json，服务端从数据库取得当前用户、角色与组织。JSON 中用户、角色、团队或负责人字段被拒绝，旧 expected_version 不会覆盖新版本。

| 接口 | JSON 输入 | 成功结果 |
| --- | --- | --- |
| POST /api/v1/tickets/resolve | ticket_id、expected_version、resolution_note | 200；状态待确认、版本加一，首次写首次解决时间 |
| POST /api/v1/tickets/reopen | ticket_id、expected_version | 200；状态处理中、版本和重开次数加一，负责人保持 |
| POST /api/v1/tickets/close | ticket_id、expected_version | 200；状态已关闭、版本加一，设置关闭时间 |

成功体沿用 `{"ticket": {...}}`。解决说明必须为 1–5,000 个 Unicode 字符，不能全部为空白，保留原文、换行、前后空格。详情工单与历史状态增加可空 resolution_note：当前工单保存最近一次，旧工单与旧历史缺失该字段时读为 null。

错误沿用现有格式：401 未认证、403 ACTION_FORBIDDEN、404 TICKET_NOT_FOUND、409 VERSION_CONFLICT/INVALID_STATE。重开无法保留合法负责人时返回 409 ASSIGNEE_UNAVAILABLE；数据库处理失败 500，提交结果未知 503。完整协议见 docs/api.md。

## 核心调用链、权限和事务

调用链为 HTTP.resolve/reopen/close→Service.Resolve/Reopen/Close→Store.Resolve/Reopen/Close→updateAction→MySQL。HTTP 复用身份、JSON、错误处理；Service 校验请求；Ticket.Resolve/Reopen/Close 处理明确业务规则；Store 沿用 READ COMMITTED、工单 FOR UPDATE、版本条件 UPDATE、insertHistory 和 Commit 后返回。

原 updateAction 固定要求 Agent，不能直接用于提单人动作。本轮只调整授权分支：认领、开始处理、解决要求当前有效 Agent 和当前有效团队成员；重开、确认关闭要求当前用户就是创建人。没有注册通用状态机、没有把任意状态或动作交给客户端选择。

提交解决要求当前用户仍为负责人、Agent 角色有效且仍属于当前有效团队，只允许从处理中进入待确认。主管／管理员不能绕过负责人要求。重开、关闭只允许创建人对待确认工单操作，任何非创建人的主管／管理员也不能使用常规接口。

重开在事务内重新锁定并检查原负责人的账号、Agent 角色和当前团队成员关系。有效时保留负责人并回到处理中；失效时拒绝整个动作，仍保持待确认，不更新次数、版本或历史。这是本轮避免“处理中但无合法负责人”的最小规则，恢复人员或转派功能仍留后续。提单人确认关闭不依赖原负责人和团队仍有效。

每次动作生成一个 UTC 微秒操作时间。状态、负责人、version、updated_at、first_resolved_at、closed_at、reopen_count 和 resolution_note 由同一个带 expected_version 条件的 UPDATE 持久化；随后用同一 sql.Tx 插入前后状态历史，最后 Commit。历史失败时所有业务字段回滚，失败方没有成功历史。

## 时间、次数与说明规则

| 字段 | 更新规则 |
| --- | --- |
| first_resolved_at | 第一次解决时为 null 才设置；重开、再次解决和关闭保留原值 |
| reopen_count | 每次成功重开加一；失败、重复或旧版本不增加 |
| closed_at | 正常确认关闭时设置为操作时间；不提前设置 |
| resolution_note | 每次解决保存最新原文；重开及关闭保留；历史保存各次快照 |
| version | 每次成功动作加一；与同版本唯一历史一起提交 |

本轮新增存储仅为 tickets 的一个可空 TEXT 字段及长度 CHECK。新库使用更新后的 schema.sql，已有库通过 db/migrations/003_resolution_note.sql 添加字段。迁移先后用旧列、全部历史与十六进制幂等键/hash 比较，确认既有业务值保留，既有说明均为 null。原历史 JSON 没有改写，迁移结果见 artifacts/m1-02b/migration-result.json。

## 实际文件和 Diff

完整变更在 artifacts/m1-02b/changes.patch，逐文件旧新哈希及增删行数在 changes-manifest.json；由本轮保存的源码基线生成，不依赖 Git 提交。

| 文件 | 本轮变更 |
| --- | --- |
| internal/ticket/lifecycle.go（新增） | 三个 Service 用例、解决说明校验、明确状态转换与创建人关系 |
| internal/ticket/model.go、service.go、errors.go | 解决说明、动作常量、Repository 方法、失效负责人错误 |
| internal/mysqlstore/actions.go | 复用动作事务，区分人员授权，重开校验原负责人，写生命周期字段 |
| internal/mysqlstore/store.go、create.go | 增加可空字段读取与新工单 NULL 写入 |
| internal/httpapi/api.go | 三个入口及现有错误映射扩展 |
| db/schema.sql、embed.go、migrations/003_resolution_note.sql | 新库基线、保留数据迁移及嵌入迁移测试资产 |
| internal/ticket/lifecycle_test.go、internal/mysqlstore/lifecycle_integration_test.go（新增） | 状态、权限、时间、说明、并发、响应丢失及迁移验证 |
| scripts/smoke_lifecycle.py（新增）、Makefile | 两条完整 HTTP 生命周期，保存请求及每步证据 |
| AGENTS.md、README.md、docs/api.md、docs/design.md、本报告 | 当前范围、接口、迁移、失效负责人规则、测试与后续入口 |

原有创建/认领/详情测试文件、路由配置、seed.sql、history.go、go.mod/go.sum、原两个 smoke 及 MySQL 隔离脚本都保持哈希不变。新字段读取和动作事务扩展属于本轮必要适配，无其他重构。

## 测试与真实业务结果

以下要求全部通过真实 MySQL 8.4.5、Go race 和 HTTP 测试。每个数据库场景使用自己创建的随机新库，结束后删除该测试库，不清空演示库。

| 验收项 | 证据 |
| --- | --- |
| 创建→认领→开始→解决→关闭 | TestLifecycleRoundTrip/reopen_false、TestLifecycleHTTPFlows/reopen_false、工单 29 smoke |
| 首次解决→重开→再次解决→关闭 | 相同测试的 reopen_true、工单 30 smoke |
| 首次解决时间不覆盖、次数正确 | 三版状态和独立 SQL 回读；版本 4/6 的 first_resolved_at 相同，reopen_count=1 |
| 说明完整持久化、详情可查 | 含中文、换行、空白、emoji 原文比对；最大 5,000 字及历史快照 |
| 非负责人不可解决 | Agent 同事、其他团队、提单人、Supervisor/Admin 均拒绝 |
| 非提单人不可重开或关闭 | 两动作按创建人关系核验，HTTP 403，不依赖伪造头部 |
| 非法源状态及关闭后操作拒绝 | 现有与新增五个状态动作都不能修改已关闭工单 |
| 旧版本及重复逻辑动作拒绝 | 每个动作成功后旧版本重试冲突，不加版本或历史 |
| 重开/关闭同版竞争只一成功 | TestReopenCloseCompetition 与真实 HTTP race，成功/冲突为 1/1 |
| 历史失败整体回滚 | 三动作真实 MySQL trigger 注入，状态/时间/次数/说明/版本保持旧值 |
| 每成功动作一条历史 | 最终直接路径 5 版/5 历史，重开路径 7 版/7 历史；before/after 连续 |
| 原两轮回归 | 原测试源码未变，全量执行；工单 1、19 的原有 HTTP smoke 继续通过 |

额外验证：十二个同版解决请求只一成功；负责人账号停用、角色变化、团队失效或成员移除时拒绝重开但允许提单人确认关闭；解决、重开、关闭提交后丢失 HTTP 响应，再以旧版本重试只返回冲突；显式迁移后旧工单仍完整可读。

真实运行服务结果如下，时间是 UTC，前端可转换为北京时间：

| 工单 | 最终版本/历史 | 重开次数 | 结果 |
| --- | --- | --- | --- |
| TP-000000000029 | 5 / 5 | 0 | 直接确认关闭 |
| TP-000000000030 | 7 / 7 | 1 | 首次解决、重开、再次解决后关闭 |

最终工单 30 历史依次是 created、claimed、processing_started、resolved、reopened、resolved、closed。版本 4 保存首次说明，版本 6 保存第二次说明；first_resolved_at 保持版本 4 时间，closed_at 等于版本 7 操作时间。每条记录的操作者、团队与前后状态均由详情和独立 MySQL 回读核对。

## 验证命令、代码量与边界

make test、make integration（含 -race -tags=integration -count=1）、make vet、make build、make smoke、make smoke-actions、make smoke-lifecycle 均通过。原始日志与最终元信息在 artifacts/m1-02b/final-integration.log、final-integration-result.json；完整 HTTP 证据在 direct-close-result.json、reopen-result.json；独立数据库结果在 database-result.json，摘要在 validation-result.json。

相对 M1-02A，实现与测试净增 **1,183 行**：Go 业务 233、Go 测试 820、SQL 6、Python smoke 124。按含注释和空行的物理行计算，文档和构建配置另计，统计见 artifacts/m1-02b/code-size.json。没有新依赖，仍仅 MySQL 驱动及其原间接依赖。

未执行 Commit 阶段网络代理故障、断电恢复、专门锁超时/死锁注入、其他平台或 MySQL 版本、性能压测。重开遇失效负责人会显式拒绝，当前无转派或主管恢复接口；可用预置配置恢复合法成员，或由提单人确认关闭。主管人工关闭、自动关闭、SLA、评论及转派不在本轮交付。

后续转派和人工关闭可直接复用 updateAction 的本地事务、当前身份、行锁、版本条件、insertHistory、错误与隔离测试夹具。转派需定义目标团队和负责人的有效性及源状态；人工关闭需独立 Supervisor/Admin 授权、必填原因与历史动作，不可借用常规提单人关闭来绕过权限。首次时间与历史说明规则已可继续复用。
