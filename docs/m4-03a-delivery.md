# TicketPulse M4-03A：安全切换的基础机制

本轮完成持久目标合同、业务冻结与事务排空、启动／就绪身份检查和搜索游标版本绑定。**M4-03A 基础机制验收通过**：七个专项真实集成测试全部通过，既有业务与同步回归为 78 个顶层测试通过，syncpipe 的 11 个测试通过；单元 race、vet 和三项 Python 测试通过。验收只覆盖基础机制，真实 ES／CK 推广和 Kafka 消费位置交接仍未实现或验证。

最终专项日志为 [focused/integration.log](../artifacts/m4-03a/runs/20261114T060013.586926Z-focused/integration.log)，既有回归日志为 [regression/integration.log](../artifacts/m4-03a/runs/20261114T055746.201138Z-regression/integration.log) 和 [syncpipe-integration.log](../artifacts/m4-03a/runs/20261114T055746.201138Z-regression/syncpipe-integration.log)。可核验的 [验收汇总](../artifacts/m4-03a/test-summary.json) 从这些原始日志和 JSON 证据生成；[收尾检查](../artifacts/m4-03a/closeout.json) 另外核对源码、旧资产、演示数据和运行端口。

## 源码基线与实际范围

实施前 live repository 位于分支 feat/m1-01-ticket-loop，没有有效 HEAD，所有项目文件未跟踪，暂存区为空。没有用旧报告的行号定位修改。先保存了 230 个源码／配置／文档文件的内容，再追踪当前入口和事务。文件版本见 [实施前哈希清单](../artifacts/m4-03a/baseline-source-manifest.json) 和 [原始源码归档](../artifacts/m4-03a/pre-m4-03a-source.tar.gz)。本轮共新增 16 个、修改 24 个文件，没有删除源码；完整清单与前后哈希见 [file-manifest.json](../artifacts/m4-03a/file-manifest.json)。

最终源码共 246 个文件，见 [最终哈希清单](../artifacts/m4-03a/final-source-manifest.json) 和 [最终源码归档](../artifacts/m4-03a/final-source.tar.gz)。[完整 Diff](../artifacts/m4-03a/full.diff) 以实施前归档为基准，已经在临时目录实际应用并逐字节比较，见 [Diff 核验](../artifacts/m4-03a/diff-verification.json)。改动文件的原文和最终副本分别保存在 artifacts/m4-03a/source/original 和 source/final。

源码事实和实测证据分开标注：[CODE] 代表当前实现，[RUNTIME] 代表本轮执行结果，[INFER] 代表设计边界，[UNKNOWN] 代表尚未验证。

| 文件 | 职责 |
| --- | --- |
| db/migrations/011_projection_control.sql、db/embed.go | 增量维护元数据及嵌入资源 |
| internal/projection/model.go、store.go、gate.go | 目标版本、报告哈希、进度归属、持久冻结／恢复和行锁 |
| internal/syncpipe/projection_control.go | 复用 M4 身份、快照校验、任务锁和全量对账；登记现有目标、准备候选、运行身份检查 |
| internal/search/identity.go、internal/analytics/identity.go、internal/syncpipe/rebuild_target.go | 共用只读 UUID 查询；保留 M4 原身份编码和恢复协议 |
| internal/mysqlstore/store.go、create.go、actions.go、internal/ticket/errors.go | 所有正常业务事务在提交前参与闸门；明确冻结业务错误 |
| internal/benchseed/import.go | 批量测试导入拒绝已安装维护元数据的库，防止绕过业务事务入口 |
| cmd/ticketpulse-control/main.go | 操作者 CLI，没有公开维护 API 或推广命令 |
| cmd/ticketpulse/main.go、cmd/ticketpulse-sync/main.go | 显式开启受管理目标检查；普通模式保持原目标配置 |
| internal/httpapi/api.go、session.go | 存活／就绪入口和冻结、旧目标游标错误映射 |
| internal/search/service.go、model.go | 新游标绑定版本，保持授权、排序、过滤和候选预算 |
| internal/analytics/service.go | 受管理看板在授权后、状态与指标读取前检查统一合同 |
| internal/syncpipe/consumer.go、analytics_consumer.go、queue.go、capture.go | 消费前有界身份检查；受管理模式禁止自动创建和初始化；捕获检查实际 Binlog 源 |
| internal/projection/model_test.go、internal/search/target_version_test.go | 合同校验、JSON 哈希和相同密钥的跨目标游标测试 |
| internal/mysqlstore/m403a_control_integration_test.go | 七项真实隔离验证、行锁观察、SIGKILL、UUID 替换和恢复 |
| internal/mysqlstore/integration_test.go、internal/syncpipe/reconcile_integration_test.go | 增加专用隔离 socket；M4-03A 环境拒绝其他 MySQL 端点 |
| scripts/m403a_environment.py、suite.py、preservation.py、package.py | 私有进程／资源归属、执行与采样、演示只读核验、Diff 和证据打包 |
| Makefile、README.md、docs/api.md、docs/design.md、本报告 | 控制二进制构建入口、公开行为与部署边界说明 |

## 实施前真实调用链

[CODE] 工单创建进入 Store.Create，其他十类动作与评论都进入 Store.updateAction。提交前在同一事务中写工单、版本和历史，评论另写关联评论行。当前没有修改标题的公开 HTTP 路由，也没有用户／团队管理写入口；这些事实来自本轮对 HTTP 路由和 mysqlstore SQL 的实际搜索。会话登录／退出只更新身份数据。批量夹具写入只允许空的测试库，本轮让它拒绝维护元数据已存在的库。

[CODE] Search API 使用启动时固定的 ES endpoint/index。ES 只提供 id/version 和排序键，MySQL 在请求内只读快照中作最终授权和版本相等复核。看板使用启动时固定的 CK endpoint/database，同时从同一 MySQL 状态库和 Kafka 的 CK 组读取进度。标准消费者使用相同配置的目标和原 ES／CK 两个独立组；每条记录下游处置成功后才提交下一 Offset。

[CODE] M4 任务、冻结输入、目标身份、报告与逐分区 next_offset 已持久化在 009 控制表中，任务执行通过连接级 GET_LOCK 排他。候选 follow 不属于标准消费组，不能把候选 next_offset 当作标准组已提交的位置。本轮没有改 cmd/ticketpulse-rebuild/main.go、M4 run/check/follow 的标记追平协议或 009／010 迁移。

定位依据使用最终源码行号；实施前对应内容可在基线归档逐字节核对。

| 事实 | 文件、symbol、行号 |
| --- | --- |
| HTTP 路由和就绪检查 | internal/httpapi/api.go，NewWithOptions，第 54 行 |
| 创建事务／闸门 | internal/mysqlstore/create.go，Create，第 16 行；acquireBusinessWrite 在第 24 行 |
| 所有动作和评论的共用事务 | internal/mysqlstore/actions.go，updateAction，第 67 行；闸门在第 73 行 |
| Store 必需模式和已见闸门记忆 | internal/mysqlstore/store.go，RequireBusinessGate，第 26 行；acquireBusinessWrite，第 28 行 |
| 事务共享锁、冻结、恢复 | internal/projection/gate.go，AcquireBusinessWrite，第 37 行；Freeze，第 110 行；Resume，第 151 行 |
| 持久目标合同 | internal/projection/model.go，Contract，第 74 行；Version，第 87 行；Validate，第 99 行 |
| 记录与进度归属原子写入 | internal/projection/store.go，Insert，第 68 行；VerifyBinding，第 112 行 |
| 初次登记、候选准备、就绪校验 | internal/syncpipe/projection_control.go，EnrollExisting，第 147 行；Prepare，第 218 行；ManagedProjection.Check，第 313 行 |
| API 显式开启 | cmd/ticketpulse/main.go，第 114 行后的 TICKETPULSE_TARGET_VERSION 分支 |
| 标准 worker 显式开启 | cmd/ticketpulse-sync/main.go，第 65 行后的受管理分支；supervise，第 199 行 |
| 实际 Binlog 源身份 | internal/syncpipe/capture.go，VerifySource，第 38 行；verifyCaptureEndpoint，第 83 行 |
| ES／CK UUID | internal/search/identity.go，Identity；internal/analytics/identity.go，Identity，第 25 行 |
| 目标游标 | internal/search/service.go，WithTargetVersion，第 28 行；cursor，第 65 行；decodeCursor，第 86 行 |

## 增量数据库合同

[CODE] 011 只创建四张 InnoDB 控制表。新库仍先安装原有业务和同步结构，再显式安装 011；普通应用和消费者启动不会自动执行迁移。迁移使用 CREATE TABLE IF NOT EXISTS 和 INSERT IGNORE，重复执行保留已有冻结。原演示库没有执行迁移。

| 表 | 保存内容 | 约束 |
| --- | --- | --- |
| sync_business_write_gate | OPEN/FROZEN、修订号、本次 freeze_id、原因、冻结时间和更新时间 | 只能有 gate_id=1；状态与冻结标识／时间一致 |
| sync_projection_versions | 版本 ID、阶段、完整 JSON 合同、新对账报告及哈希、冻结修订号和时间 | 版本 ID 唯一；生成列唯一约束最多一个 ACTIVE |
| sync_projection_progress_bindings | ES／CK 组件对应的目标版本、writer/进度／状态归属 | 组件唯一；版本外键；与 ACTIVE 在同一事务写入 |
| sync_projection_control_log | 冻结、恢复、登记结果及引用 | 与控制状态一并提交 |

[CODE] 一个版本包含源 MySQL UUID 和 schema，Kafka Topic 名、UUID 与逐分区集合，ES endpoint/index/index UUID，CK endpoint/database/ticket_events UUID/ticket_states UUID。当前写入者分别记录组或 M4 任务，以及进度表和状态组件。候选额外保存任务 ID、原冻结输入哈希、原 M4 报告哈希、每分区 next_offset 和后续拟使用的标准组。报告、合同 JSON 哈希保留大整数精度，并兼容 MySQL 的 JSON 键顺序和空白转换。

本轮只允许下面两种创建，不提供后续阶段推广。

| 创建方式／阶段 | 必要条件 | 能否供 API／标准消费者使用 |
| --- | --- | --- |
| 首次登记现有目标 → ACTIVE | 持久冻结；已有 CDC 和 CK bootstrap；两组每分区已有提交位置；目标不是 M4 候选；当前内容全量对账一致；前后身份一致 | 可以，但仍须每次身份检查通过 |
| 完成的双目标 M4 候选 → PREPARED | 持久冻结；取得原 M4 任务锁；原任务 completed；冻结快照哈希正确；当前 UUID 和位置仍有效；重新全量对账一致 | 不可以 |
| PREPARED → ACTIVE | 本轮无入口 | 不支持，等待 M4-03B |

[RUNTIME] 测试先完成 M4 Gold 重建并保存 consistent，再创建新工单。旧报告仍为 consistent，但本轮 prepare 拒绝陈旧候选；同一候选经原 M4 真实追平后，新的准备报告覆盖 11 张工单、47 条历史、6 次首次解决，差异为 0。准备前后候选位置均为 0/1/2 分区的 10/8/8，标准组位置没有迁移。活动 follow 持有原任务锁时准备也被拒绝，候选不能绕过 prepare 用 enroll-existing 变成 ACTIVE。证据见 [prepared.json](../artifacts/m4-03a/runs/20261114T060013.586926Z-focused/verification/TestM403APreparedRequiresFreshFrozenContentAndPreservesOffsets/prepared.json)。

[INFER] PREPARED 是“这个冻结修订和观察时刻，候选身份及内容已核验”的持久证据。恢复业务、重启 follow 或新增消息后，该记录不会自动成为当前有效切换授权。旧记录保留供审计，新冻结需要新的版本 ID 和新检查。候选到标准组的交接始终标为 handoff_verified=false。

## 冻结如何排空事务

[CODE] 两个实际业务事务入口都先锁闸门，再锁用户、工单、目标团队／成员。共享闸门记录锁一直留在原事务里；业务更新、历史、评论或提交失败时一并回滚。冻结获取同一行的排他锁，必须等共享锁全部释放才能写 FROZEN。后来到达的写事务在排他锁之后重新读取当前状态，并返回 BUSINESS_WRITES_FROZEN，不会在一次外部检查后继续提交。

冻结事务提交后还要读回状态和修订号，确认之前若状态被另一个显式维护操作改变则返回失败。没有自动解除、租约或进程存活条件。恢复写入必须提供当前 freeze_id，在排他锁内更新并读回 OPEN。提交结果不明时要求检查持久状态，不把超时当作冻结已经成功或已经解除。

[RUNTIME] 实验先锁住一个工单行，让真实评论事务取得闸门共享锁后等待工单锁；同时启动冻结和 12 个创建请求。数据库观察到 S,REC_NOT_GAP 已授予、X,REC_NOT_GAP 等待。时间线如下，全部为同机 UTC 实测。

| 时刻 | 观察 |
| --- | --- |
| 06:00:33.155616 | 启动冻结 |
| 06:00:33.168355 | 释放测试工单锁，真实评论继续更新并提交 |
| 06:00:33.169851 | 闸门记录写入 FROZEN |
| 06:00:33.170665 | 冻结提交并读回确认 |

12 个并发创建全部明确拒绝；冻结确认后再发 20 个写请求，全部拒绝；历史没有超过冻结边界的新增，已在途评论恰好保存一条评论和对应新版本历史。显式恢复后可创建下一张工单。完整锁观察与时间见 [drain-race.json](../artifacts/m4-03a/runs/20261114T060013.586926Z-focused/verification/TestM403AFreezeDrainsActualTransactionAndConcurrentCreates/drain-race.json)。这证明本轮并发场景的排空边界，不是长期容量或高并发延迟承诺。

[RUNTIME] 冻结期间创建／创建重试、认领、开始、解决、关闭、重开、负责人转派、团队转派、管理重开、人工关闭和评论均被拒绝，业务行计数不变。详情、基础列表、存活／就绪、登录和退出可用。恢复后评论为 v2，两条历史、一条评论。已见闸门的 Store 在表丢失后拒绝写入，不会降级为旧模式。见 [coverage.json](../artifacts/m4-03a/runs/20261114T060013.586926Z-focused/verification/TestM403ALegacyGateAndFrozenBusinessCoverage/coverage.json)。

[RUNTIME] CDC 和内部标记不参与业务闸门。冻结时一个 MySQL 源标记到达两组的全部六个分区水位，捕获检查点向前推进，MySQL／ES／CK 仍全量 consistent。原始位置见 [internal-progress.json](../artifacts/m4-03a/runs/20261114T060013.586926Z-focused/verification/TestM403AFrozenCDCMarkersAndCheckpointsContinue/internal-progress.json)。

[INFER] 安装与部署必须先隔离全部业务写入者，安装 011，再启动参与闸门的新应用；启用必需检查后才能使用冻结确认。旧模式容许未安装 011 的库，这保留原环境兼容，但不能在迁移安装期间约束已经越过缺表检查的事务。旧二进制、人工直连 SQL 和仓库外部写入者必须由部署流程排除。登录会话是明确豁免的身份数据，不改变工单投影；没有公开账号或团队管理写接口。

## 启动和就绪如何拒绝错配

[CODE] 显式开启受管理版本后，应用用实际创建的 ES／CK 客户端、实际 MySQL 连接和实际 Kafka Queue 检查完整合同。运行中固定版本 ID 和不可变合同摘要，读取进度绑定再核对组位置和已有 bootstrap。PREPARED、不匹配配置、目标 UUID 改变、状态归属改变、身份查询失败都不能报告 ready。API 的 Search／Analytics 请求也在读取投影和状态之前检查，基础 MySQL 读取继续保留。

[CODE] 受管理消费者只使用已有标准组和已有目标：禁止自动创建 Topic／索引／空间，禁止 bootstrap、snapshot、replay、cleanup 和迁移类 CLI 模式。每个有界消费批次前检查，捕获在已提交事务发布前检查；身份不一致是监督器的明确终止错误。CDC 启动还使用实际捕获 TCP 地址和捕获账号查询服务器 UUID，不能仅用同步状态库的 UUID 替代。

[RUNTIME] 正确 ACTIVE 合同的真实 API 进程可返回 ready。错误 ES／CK 名称的真实 API 和消费者 CLI 都在监听或组加入前退出。ES 同名重建、CK 表在保留原表后新建同名表、报告哈希被破坏、状态组件指向 ES、分区集合改变以及身份端点不可访问均使就绪返回 503。CK 原表恢复后身份可以再次通过。见 [target-and-cursor.json](../artifacts/m4-03a/runs/20261114T060013.586926Z-focused/verification/TestM403AActiveIdentityReadinessAndSameKeyCursor/target-and-cursor.json)。

[RUNTIME] 受管理 CDC、ES 和 CK 在原组／原目标上继续消费，显式恢复后的真实评论变成 v2，全量对账差异 0；再次冻结时内部水位仍推进。私有 CK UUID 变化后三个 worker 均返回身份错误，观测时刻距替换约 0.96 秒。见 [managed-workers.json](../artifacts/m4-03a/runs/20261114T060013.586926Z-focused/verification/TestM403AManagedExistingWorkersResumeAndLateIdentityLoss/managed-workers.json)。该时间包含测试顺序等待的观测，不能作为故障恢复 SLA。真实未知目标检查失败也验证了保守拒绝，没有读取其他投影作为替代。

[UNKNOWN] 运行检查有检查与使用之间的窗口；它没有给任意外部删表／重建增加分布式锁。固定版本内管理检查不等于消费位置交接锁。已建立 Binlog 流之后替换其 TCP 服务、故障环境的网络／TLS 切换、长期 managed 运行性能仍未测试。

## 搜索游标

[CODE] 新游标把目标字段与用户、组织、查询摘要、过期时间和排序位置一并 AES-GCM 加密。受管理目标字段为版本 ID 加不可变合同摘要；普通模式为稳定的 endpoint/index 摘要。受管理 API 必须配置稳定游标密钥，避免重启随机密钥把目标语义掩盖掉。普通模式继续兼容原启动方式。

[RUNTIME] 使用完全相同的密钥重建同版本 Search Service，第二页返回正确下一张工单。用另一真实隔离 ES 索引和另一目标版本读取同一旧游标，HTTP 明确返回 409 / SEARCH_CURSOR_TARGET_CHANGED。单元测试另证明拒绝发生在第二目标查询之前。升级前不含目标字段的旧游标同样要求重新搜索。没有改变最终 MySQL 授权、业务版本相等复核、过滤、排序、lookahead、last-returned cursor 或 2000 候选上限。

## 隔离环境、命令与故障证据

所有实际写入、DDL、UUID 替换与 SIGKILL 均发生在新建 .local/m4-03a。历史测试环境没有重启，演示环境只读观察。启动前验证未占用端口；每次运行前再次检查 MySQL UUID／datadir、Kafka cluster ID／data、ES cluster／data／进程、CK config／进程，再允许创建随机测试库与目标。账号只保存在私有权限环境文件，日志及交付不输出 Cookie、Bearer 或捕获密码。

| 组件 | 版本、配置与私有端口 |
| --- | --- |
| MySQL | 8.4.5，33311，64 MiB buffer pool，ROW/FULL，sync_binlog=1，提交日志持久化 |
| Kafka | 4.4.0 / Temurin 21.0.13，19100／Controller 19101；专用单节点，本轮不验 HA |
| ES | 8.17.5，19205／19305，512 MiB heap，单节点 |
| CK | 26.10.2.103，18128／19006，4 GiB 内存跟踪预算，原 M3 两表模型 |
| API／控制 CLI | 本轮源码的 Go race 二进制，位于 .local/m4-03a/bin；API 用独立临时 loopback 端口 |

进程、配置与资源证据见 artifacts/m4-03a/environment，以及每个 runs 子目录的 environment.json。安全预算为测试磁盘最多 8 GiB、剩余磁盘至少 20 GiB、可识别私有进程 RSS 最多 8 GiB；每两秒采样，达到预算会终止本轮测试。它是原生 macOS 的测试保护，不是 cgroup 或跨机资源隔离。最大实际观测值见 test-summary.json。

复现仅使用新的专用环境：

~~~bash
python3 scripts/m403a_environment.py start
python3 scripts/m403a_environment.py verify
python3 scripts/m403a_suite.py focused
python3 scripts/m403a_suite.py regression
python3 scripts/m403a_suite.py static
python3 scripts/m403a_preservation.py
python3 scripts/m403a_environment.py stop
python3 scripts/m403a_package.py
~~~

每次 suite 都建立新的时间戳目录，实际 go build/go test 命令、开始／结束时间、退出码及资源样本保存在对应 JSON；没有调用连接原演示库的 make integration／search／analytics 包装器。所有历史证据保留，最早错误输入和资源采样失败也没有覆盖。

| 验收项 | 实际观测与结果 |
| --- | --- |
| A 未开启功能兼容 | 缺 011 的旧库可正常创建；M1–M5 78 项业务／同步回归 PASS |
| B 冻结与在途事务 | 真实评论共享锁与冻结排他锁等待、12 个并发创建；PASS |
| C 冻结后拒绝业务写入 | 12 类写入／创建重试核验及 HTTP503；PASS |
| D 只读可用 | 详情、列表、health、ready、登录／退出；PASS |
| E 内部同步不冻结 | 两组全部分区标记、捕获检查点继续推进；PASS |
| F 冻结后进程硬杀 | CLI 输出管道阻塞后 SIGKILL、冻结应用 SIGKILL 和重启；持久 freeze_id 不变；PASS |
| G 配置错配 | 四个真实 API／消费者启动拒绝用例，运行 ready503；PASS |
| H UUID 改变 | ES 同名新索引、CK 同名新表独立拒绝；PASS |
| I 同版本分页 | 相同密钥／版本重建服务，第二页正确；PASS |
| J 跨版本游标 | 第二真实私有索引拒绝旧游标，HTTP409；PASS |
| K 显式恢复 | 精确 freeze_id 恢复，工单、历史、评论无丢失，原 managed workers 追平；PASS |
| L 回归 | 78 项 MySQL 业务集成，11 项 syncpipe，单元 race／vet／Python；PASS |
| 额外候选正确性 | 陈旧报告、活动 follow、候选登记绕过被拒绝；新的完整对账 PASS |
| 额外登记崩溃 | 提交后 CLI SIGKILL 保留 PREPARED；提交前 KILL CONNECTION 不留版本／控制日志；PASS |

冻结和登记的提交后硬杀，使用填满的 stdout 管道让真实 CLI 在数据库已提交而结果尚未写完时保持存活，再读取持久状态并发送 SIGKILL。应用硬杀使用真实独立 HTTP 进程；重启后业务仍被拒绝，显式恢复才可继续。见 [process-crash.json](../artifacts/m4-03a/runs/20261114T060013.586926Z-focused/verification/TestM403AFreezeSurvivesCLIKillApplicationKillAndResume/process-crash.json) 和 prepared.json，源码中没有“模拟成功”的测试分支。

原 M3 十工单 Gold 已在原有回归重新验证：新增 10、首次解决 6、平均 60600 秒、当前积压 5。M4 测试还复跑了真实 HTTP 重叠、已写投影但未保存进度、SIGKILL 重建恢复、同名 Topic 身份变化、Kafka 保留边界丢失、损坏目标、检查超时与重新跟随。M5-03 的重复／迟到版本／同键冲突和 M5-04 的成员变化／旧团队拒绝／分页回归均通过。M4-02 三 Broker 故障矩阵只读复用历史 PROCESS_FAILOVER_VERIFIED 证据，本轮未重跑其 Broker 注入；本轮相关 Kafka 配置和重平衡合同在普通回归中执行。

一次专项失败来自不存在的测试 token，HTTP401 尚未进入冻结入口；改成既有合成账号后通过。一次回归观测异常来自 CK 合并删除临时目录，磁盘枚举因此 FileNotFoundError；修正采样并重跑通过，失败轮不计完整 PASS。实现和测试过程中出现的编译问题已修正，所有最终检查退出码为 0。

## 维护 CLI 与安装策略

维护凭据使用 TICKETPULSE_CONTROL_DSN，单独由操作者持有。正常 API／worker 数据库账号需要 SELECT 控制合同与闸门，操作者还需 011 控制表写权限和显式迁移权限；捕获 UUID 查询需要实际 Binlog 账号能读取服务器变量。新增源码没有自动授权、自动迁移或写原服务配置。

| 参数／命令 | 含义 |
| --- | --- |
| TICKETPULSE_TARGET_VERSION | API／标准消费者显式固定 ACTIVE 版本；未设则保持原目标配置模式 |
| TICKETPULSE_REQUIRE_WRITE_GATE=true | 单独要求 API 启动必须存在有效闸门；managed 自动要求 |
| TICKETPULSE_CURSOR_KEY | 稳定 32 字节密钥的 64 位十六进制配置；managed 必须提供 |
| ticketpulse-control -mode=migrate | 只安装 011 控制表 |
| -mode=status | 读持久闸门 |
| -mode=freeze -reason=... | 排空并持久冻结，无自动解除 |
| -mode=resume -freeze-id=... | 精确冻结身份恢复并读回确认 |
| -mode=enroll-existing -version=... | 对现有目标全量新检查后首次登记 ACTIVE |
| -mode=prepare -version=... -job=... | 在持久冻结下核验完成的 M4 候选并登记 PREPARED |
| -mode=version -version=... | 读持久合同／报告 |
| -mode=check -version=... | 实时身份／归属检查，只接受 ACTIVE，不重新证明内容追平 |
| -wait | 有限操作截止时间，默认 30 秒、最多 5 分钟 |

推荐安装顺序是先在隔离环境验证部署凭据，再由操作者控制所有业务写入入口，显式安装 011，启动要求闸门的新应用，完成持久冻结。现有同步进程继续追平后登记现有 ACTIVE，随后应用和原标准消费者使用相同版本配置，核对就绪，再明确恢复写入。这个顺序只登记已有目标，不推广候选；本轮没有在演示环境执行这些步骤。

## 保全与后续边界

本轮新增 artifacts/m4-03a 目录，逐字节保全了实施前 5183 个历史证据文件，总量约 3.35 GB。核验见 [previous-evidence-preservation.json](../artifacts/m4-03a/previous-evidence-preservation.json)。演示只读内容核验还包括账号、会话、团队与成员关系，9 张 MySQL 表、ES 外部版本及全部 CK 物理行均与原 M4 基线一致；演示库中 011 的四张表数量为 0。没有冻结演示、停止演示进程、修改 alias、重置消费者组。仅停止本轮归属已核验的私有进程，保留其数据目录；暂存区为空，没有提交或推送。

[UNKNOWN] 本轮没有验证长期高并发冻结排空、十万级候选登记耗时、managed 元数据检查对搜索尾延迟的影响、MySQL 服务／主机硬故障、跨机／网络故障域、Docker／Linux、混合旧新写入进程的在线迁移，也没有再次执行 M4-02 的三 Broker 故障矩阵。现有四 GiB CK 配置用于正确性与身份实验，没有据此扩大 M5 性能结论。

M4-03B 仍需明确“哪一次冻结对应哪组逐分区源标记”，在最新新对账后停止候选 follow 和原写入者，验证 Topic 身份及每分区位置，安全保存交接意图并迁移原组位置，再协调读目标、写目标和状态归属。必须测试切换 CLI 中途崩溃、ES／CK 分步错配、应用与消费者启动顺序、旧投影滞后和回滚条件。本轮的 UUID 检查不能替代这一交接协议，PREPARED 的报告不能授权恢复后的新写入切换。

已实现并真实验证的是 M4-03A 基础合同和维护边界。它满足本轮 A–L 验收，**不代表候选安全推广、消费位置交接或生产高可用已经通过验收**。后续实施从 M4-03B 的交接设计和真实故障场景开始。
