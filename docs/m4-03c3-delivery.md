# TicketPulse M4-03C3：受控读目标回滚与 M4 故障验收

本报告回答：已经推广到候选 B 后，能否重新证明旧投影 A 完整、可靠停止 B API，并在可恢复的持久协议下切回 A。验收范围是 macOS 同机、操作者独占的进程清单和私有 MySQL/Kafka/ES/CK 实例。

[RUNTIME] 最终状态为 M4-03C3_PASS。真实结果为 C3 10项、C2 14项、C1 12项、M4-03A/B 7/21项、M1–M5 89项通过；Go race、go vet与3项Python unittest通过。最终保留环境真实完成 A1→B2→A3，A3 API及两套消费者持续运行。完整Diff逐字节恢复、9,334个历史文件哈希和原演示保留核验全部通过；见 [test-summary.json](../artifacts/m4-03c3/test-summary.json) 和 [closeout.json](../artifacts/m4-03c3/closeout.json)。

证据标记：[CODE] 当前源码；[RUNTIME] 已真实执行；[INFER] 设计取舍与边界；[UNKNOWN] 尚未验证。源码以本轮基线和最终内容哈希为准，历史报告只用于定位。

| 术语 | 含义 |
| --- | --- |
| A / B | 原登记 ACTIVE 的 legacy 投影 / M4-03B 已接续的 PREPARED 候选 |
| 读选择与修订号 | 013 单例同时决定 ES、CK 和状态读器；每次已提交切换只递增一次 |
| 新鲜证明 | 当前冻结、当前操作的精确源标记、真实逐分区提交及完整对账 |
| 反向引用 | 回滚或显式恢复指向当前已提交操作，保留连续审计链 |
| 进程观察 | 主机、PID、启动时间、可执行哈希、监听、boot 与 Kafka 成员的组合证据 |

## 1. 基线、改动范围与兼容

[CODE] 分支为 `feat/m1-01-ticket-loop`，没有有效 HEAD，index 为空。本轮基线包含 288 个文件，与 M4-03C2 最终源码逐字节一致。实施前源码清单、归档和核对分别在 [baseline-source-manifest.json](../artifacts/m4-03c3/baseline-source-manifest.json)、[baseline-source.tar.gz](../artifacts/m4-03c3/baseline-source.tar.gz)、[preflight-source.json](../artifacts/m4-03c3/preflight-source.json)。既存文件全未跟踪，没有用空 Git Diff 判断改动。

最终源码300个文件，本轮新增12个、修改22个、删除0个，共34处文件变更。测试驱动和交付文档计入源码清单；历史artifacts和私有运行目录单独保存，不混入源码归档。

| 文件 | 本轮职责 |
| --- | --- |
| [015_projection_read_rollback.sql](../db/migrations/015_projection_read_rollback.sql)、[embed.go](../db/embed.go) | 增量反向引用与旧写入者观察表；显式安装 |
| [projection/read_rollback.go](../internal/projection/read_rollback.go) | 安装完整性、读修订号连续链和原 ACTIVE / 候选 Handoff 校验 |
| [projection/read_control.go](../internal/projection/read_control.go)、[read_execution.go](../internal/projection/read_execution.go)、[read_api.go](../internal/projection/read_api.go) | 反向意图、证明和审计关联；取消后恢复守卫；排除显式接替的历史未完成记录 |
| [syncpipe/read_reverse.go](../internal/syncpipe/read_reverse.go) | 同一 Begin 协议的反向引用、锁、CAS 和已提交失败的显式恢复 |
| [read_switch_control.go](../internal/syncpipe/read_switch_control.go)、[read_switch_commit.go](../internal/syncpipe/read_switch_commit.go) | 通用新鲜证明复用、合法取消、A/B 原子读选择提交 |
| [read_legacy_process.go](../internal/syncpipe/read_legacy_process.go)、[read_projection.go](../internal/syncpipe/read_projection.go) | legacy 实际进程与组成员证明；原始 FetchOffsets 与保留范围 |
| [consumer.go](../internal/syncpipe/consumer.go)、[analytics_consumer.go](../internal/syncpipe/analytics_consumer.go)、[queue.go](../internal/syncpipe/queue.go) | 仅在 015 安装后登记旧消费者实际身份和成员；保留写入、进度、提交顺序 |
| [read_api_drain.go](../internal/syncpipe/read_api_drain.go)、[read_api_ready.go](../internal/syncpipe/read_api_ready.go) | 受控 API 实际退出、新 boot 验收与原冻结恢复；支持 A 无 Handoff |
| [read_cancel_resume.go](../internal/syncpipe/read_cancel_resume.go) | 取消失败回滚后，重新验证当前 B 才允许显式恢复业务 |
| [ticketpulse-control/main.go](../cmd/ticketpulse-control/main.go) | 015 安装、精确反向引用和取消后恢复 CLI |
| [m403c3_read_rollback_integration_test.go](../internal/mysqlstore/m403c3_read_rollback_integration_test.go) | 真实往返、拒绝矩阵、SIGKILL、重放、权限、游标和保留夹具 |
| 既有集成测试夹具 | C3 私有实例准入、可选资源保留、实际 worker 端点；原断言保留 |
| [m403c3_environment.py](../scripts/m403c3_environment.py)、[suite.py](../scripts/m403c3_suite.py)、[live.py](../scripts/m403c3_live.py)、[preservation.py](../scripts/m403c3_preservation.py)、[package.py](../scripts/m403c3_package.py) | 私有实例与预算、测试互斥、保留环境、演示核验、完整源码交付 |

[CODE] 011–014 原迁移、不可变 Contract、候选 Handoff、候选 writer 归属、CDC、M4 重建与 Reconcile、Search 业务过滤、MySQL 授权、M3 统计 SQL 保持基线字节。API 统一读选择和修订号游标接线沿用 C2，未改变工单详情的数据源。具体未变文件在 [frozen-contract-preservation.json](../artifacts/m4-03c3/frozen-contract-preservation.json)，全部改动及准确 symbol/行号在 [file-manifest.json](../artifacts/m4-03c3/file-manifest.json)、[final-source-facts.json](../artifacts/m4-03c3/final-source-facts.json)。

015 只增加两个可空反向关联字段和一张旧消费者观察表，没有更改业务表或源检查点。安装要求 014 已显式完成；原未安装环境继续走兼容路径。安装来源丢失、字段只存在一半、表缺失都会拒绝读控制和恢复。字段完整的未知安装结果可显式重试，字段不完整则要求明确修复，不会悄悄退回旧行为。

## 2. 回滚使用同一持久协议

[CODE] 正常回滚仍是 `PLANNED → COMMITTED → API_READY → RESUMED`。回滚引用当前已提交推广操作，要求当前读选择确实为 B、目的为 A、新冻结与期望修订号匹配。操作 ID 的方向、目的、修订号和反向引用不可变化；重复同一操作从持久结果恢复，其他操作通过现有控制锁和 MySQL 行锁序列化。

原 ACTIVE/PREPARED 继续表示登记与写入归属。回滚只更新 013 的读选择，A 的 Contract 指纹不用修改，B 的 VERIFIED 历史保持原样。正常往返修订号为 1→2→3；不会将 A 的旧修订号 1 写回。读回会验证从初始启用日志到当前提交的完整连续链，缺口、分叉、失去当前引用、反向关联或审计不一致均拒绝。

| 阶段 | 持久事实 | 失败后的允许动作 |
| --- | --- | --- |
| PLANNED | B 仍为权威读目标，新业务冻结；新投影读准入关闭 | 重验同一精确标记；安全取消后验证 B 健康，再显式恢复 |
| COMMITTED | 单事务将 B→A、修订号 +1、操作提交和审计落库 | 恢复 A API；或显式创建指向该已提交操作的反向恢复，重新证明目的 |
| API_READY | 本次 A boot 的真实进程、HTTP、权限和完整对账证据 | 新 boot 必须重新观察；历史就绪不直接允许 Resume |
| RESUMED | OPEN 与 RESUMED 同事务提交 | 幂等查询已提交结果；后续回滚需要另一轮冻结和证明 |

[CODE] 已提交后不能取消。首版增加的最小恢复机制是：新建显式反向操作并链接失败的已提交操作，旧记录保留原 COMMITTED/API_READY 阶段，以 `superseded_by` 和审计标明受控接替。新操作同样要验证、排空、提交、就绪和恢复，修订号继续递增。若取消新 PLANNED 恢复，恢复旧的未完成指针、保持冻结；取消重试返回已落库的取消，不会取消其前置操作。

[RUNTIME] 故障路径完成 A1→B2→A3；A3 启动失败后显式恢复 B4，两个目标仍一致。也验证了取消这一显式恢复后普通 Resume 被拒绝。证据见故障矩阵中“已提交失败的显式恢复”，没有手工覆盖读选择或阶段字段。

[RUNTIME] 最终保留环境的两次实际提交和就绪恢复轨迹如下，时间均为UTC。原操作、执行证明、API boot、观察心跳及22条读控制审计的原始读回在 [live-operation-timeline.json](../artifacts/m4-03c3/live-operation-timeline.json)。

| 操作 | 实际选择提交 | 新API就绪观察 | 显式恢复业务 |
| --- | --- | --- | --- |
| forward，A1→B2 | 2026-11-15 17:04:53.936673 | 17:04:54.254593 | 17:04:56.341741 |
| rollback，B2→A3 | 2026-11-15 17:05:00.304217 | 17:05:00.849564 | 17:05:03.648827 |

A1 boot `62750111d6b2359b245d731ad168c93f` / PID19504、B2 boot `aed6f0dfde3bb5098959407c5aaad536` / PID19556均在下一次选择提交前实际退出；A3 boot `be64025a1e048a19183e8423f6b3099c` / PID19672存活。登记阶段仍为A ACTIVE、B PREPARED。

## 3. 旧 A 必须获得本次新鲜证明

[CODE] 回滚复用 C1 证明入口及 C2 提交重验。当前冻结下，将唯一标记与回滚操作在 MySQL 同事务绑定，经正常 CDC 发到每个 Kafka 分区。检查先确认当前目标身份、真实提交与写入者，再按精确标记查已应用投递、读取真实 Kafka 消息，确认事件身份、源事务和标记时间完全一致。全部分区 committed 必须严格越过该消息，并位于实际保留范围。

A 解析原 legacy 两个 Group ID 与旧 delivery/watermark/heartbeat，B 解析版本化组和状态。正式证明直接使用原始 FetchOffsets，缺失提交位置拒绝。Queue.Progress 原有缺失位置回退保留起点的行为没有拿来证明回滚可用。只有完整对账、源前后相同、ES/CK 冲突为零后，证明及摘要才持久保存。提交前再次重验源、UUID、同一标记、writer、提交位置、隔离状态、完整对账和旧 API 退出。

证明绑定 operation、反向引用、freeze ID/revision、from/to、Contract hash、读修订号、精确标记、各分区实际消息及位置、目标 UUID、CDC 位置、完整报告和验证时间。源或 writer 改变时已有历史报告不能无条件继续使用；消费重启后的新进程需要重做当前证明。

[RUNTIME] 主回滚证明覆盖 11 工单、49 条规范历史，提交时源内容相同；业务恢复后两套投影覆盖 12 工单、51 条规范历史，完整 Reconcile difference_count=0、冲突=0。首次解决仍为 6、重开仍为 2。物理 CK 状态行数允许不同，验收使用规范事实、当前最大有效版本及独立 MySQL 指标，未以物理行数相等代替正确性。

主流程本次 marker 为 `db5108592b72110f7d4e5ba2be401186`，冻结为 `dbc51e8c77736416fdb0497299043049` / revision 5，操作 `rollback`，from read revision 2。完整证明、原始位置和对账分别见 [fresh-rollback-proof.json](../artifacts/m4-03c3/fresh-rollback-proof.json)、[legacy-raw-fetch-offsets.json](../artifacts/m4-03c3/legacy-raw-fetch-offsets.json)、[full-reconcile.json](../artifacts/m4-03c3/full-reconcile.json)。

| 分区 | 精确标记消息 Offset | A-ES committed | A-CK committed | 保留范围 |
| --- | --- | --- | --- | --- |
| 0 | 43 | 45 | 45 | [0,45] |
| 1 | 34 | 36 | 36 | [0,36] |
| 2 | 28 | 30 | 30 | [0,30] |

原始 JSON 向量与摘要见 [proof-vector.json](../artifacts/m4-03c3/proof-vector.json)。提交重验的证明摘要为 `2226cacb484a2ba16dced512671c24e1fa8de20bb81ffd477afa431a5307840c`，完整报告摘要为 `8bcac362a134903d71a47db0f5bcd6330cd49cb16b1110aaed51348bf7c58152`。这些值来自最终专项真实输出。

## 4. legacy 写入者观察的保证与限制

[CODE] A 不使用候选 B 的 owner 冒充写入权。015 启用后，原 ES/CK 消费者分别登记启动 nonce、源、Topic、真实目标 UUID、主机/PID/启动时间/可执行哈希；从同一个实际 Kafka 客户端获取 member 和 generation，持续保存心跳。每个组件要求一个真实活进程、一个 Stable 组成员、全部分区分配、loopback 客户端地址、当前 worker 状态和零未处理/隔离/未知投递。证明前后和提交前的身份必须一致。

这个观察不加入额外组，不初始化或提交 Offset，不取得候选租约。消费者仍按投影成功→进度持久化→Offset Commit 顺序处理，ES 严格外部版本与 CK 规范去重逻辑保留。新观察用每个消费者的局部 Queue 副本，避免 ES/CK 共用队列时串入另一组件身份。

[RUNTIME] 已验证旧消费者停用、删除观察记录、原始提交缺失、越界、日志保留起点越过提交、组/Topic/ES/CK 身份改变、隔离、内容缺失与同版本冲突均拒绝回滚。A 的证明中 `owner` 缺省，`legacy_process` 为实际原进程，两个组分别证明。候选 owner 和状态仍使用原 M4-03B 机制。

[INFER] 这些检查组合证明的是同机受控旧消费者当前仍在正确目标工作，不能严格隔离不合作的外部 writer、跨主机网络分区或后端请求已经发出时的抢占。未理解 015 的旧消费者缺少这一证明，会明确拒绝回滚，须由操作者受控升级/重启后再验证。没有为 A 引入新的跨进程租约系统，也没有宣布跨主机 fencing。

## 5. API 排空、原子读选择与恢复

[CODE] 回滚继续使用 C2 受控进程清单。PLANNED 拒绝 B 的新 Search/Analytics，已通过准入的请求必须完成或进程退出；详情及基础列表继续用 MySQL。操作者显式停止所有受控 B API，控制器核对真实 PID 退出、重枚举范围和停用的自动重启入口。发送信号、端口关闭、readyz503、固定等待或 named lock 都不构成退出成功。

单个 MySQL 事务重新核对冻结、from、预期 revision、完整证明和实际退出后，同时提交 A 的读版本/Contract/handoff、新修订号、COMMITTED 及审计。提交结果未知时读回持久事实，重复同一 operation 不再递增。A/B ES 和 CK 从未分开提交。

新 A API 固定 A/hash/revision3/boot，环境只核对预期配置。ES、CK、Search freshness 和 Analytics status 都从同一选择解析：A 使用 legacy 状态，B 使用版本化状态。每次新投影请求核对持久选择和准入，旧 B 进程在提交后不能继续成功服务旧目标。受控退出是本轮混读窗口的实际收口条件。

[RUNTIME] 真实 B 在途 Search 已通过准入并到达私有透明 ES 代理时，PLANNED 不将它误当排空；进程未退出时提交拒绝。真实 SIGKILL 后观察 PID 退出，才能提交 A。C2 双监听端口、新旧二进制、正常 Shutdown、超时强制关闭和真实重启入口专项重新运行。原不了解选择器的 API 必须被受控实际停止，不能靠新增表约束。

就绪观察读取实际 `/readyz`、主管只读目标诊断、Search、Analytics 和当前普通用户授权探针。Search 首屏与独立 MySQL 授权页比较字段、版本、ID、排序和分页；Analytics 与独立 MySQL 指标和完整对账比较。历史 API_READY 不能代替当前存活，新 boot 要重新观察；Resume 再次验证当前进程和真实请求，OPEN 与 RESUMED 同事务提交。证据见 [api-drain.json](../artifacts/m4-03c3/api-drain.json)、[api-ready.json](../artifacts/m4-03c3/api-ready.json)、[api-resumed.json](../artifacts/m4-03c3/api-resumed.json)。

## 6. 权限、游标与真实业务

[RUNTIME] 同一环境连续完成 A 正常读→B 已重建接续→A→B→B 期间 HTTP 创建/评论/团队转派→两套追平→新冻结→B→A→A 期间新增评论与创建。旧团队详情返回403、新团队返回200；Search 使用当前 MySQL 授权和版本，运营看板主管权限及 PRD Gold 未改变。Gold 日期窗口中的新增仍为10、首次解决6、平均首次解决60600秒；当前积压在新增待处理后按已知业务事实增加。

新旧游标使用同一个密钥、用户和查询。有效 A1 游标在 A3 返回409，B2 游标在 A3 返回409，新的 A3 游标可以继续分页；人为构造的真正过期游标仍返回400。原游标目标错误代码为 `SEARCH_CURSOR_TARGET_CHANGED`，日志保存了实际代码和请求关联 ID，没有记录游标密文或凭据。Contract 指纹没有改动。原排序、search_after、lookahead、page_size+1、2000 候选预算保留。

[RUNTIME] 从真实 Kafka 历史读取同一工单4的首次与再次解决消息，将原字节各重发两次。四个消费组实际越过重放位置，两套完整规范事实、首次解决与 Gold 保持一致，回滚仍成功。见 [replay-idempotence.json](../artifacts/m4-03c3/replay-idempotence.json)。

## 7. 失败矩阵、崩溃恢复与失败尝试

故障仅注入 C3 新随机业务资源，不改演示或历史资源。[fault-matrix.json](../artifacts/m4-03c3/fault-matrix.json) 对应用户16项要求；[refusal-matrix.json](../artifacts/m4-03c3/refusal-matrix.json) 列出14项拒绝和原始错误；[sigkill-state-timeline.json](../artifacts/m4-03c3/sigkill-state-timeline.json) 保存实际被杀进程、冻结、选择和操作。

| 故障组 | 真实观测与恢复 |
| --- | --- |
| PLANNED、标记、证明提交前后 | 6次控制子进程 SIGKILL；未提交则重做，已提交则读回；重试沿用持久精确标记 |
| 读选择、API_READY、Resume 提交前后 | 6次控制子进程 SIGKILL；读目标/修订号/阶段原子；就绪进程再核验；没有提前 OPEN |
| B 未退出或在途请求 | 排空与提交拒绝；实际退出后按原操作接续 |
| A 启动失败、API_READY 后被杀 | 继续冻结；原 boot 拒绝恢复，新 boot 重新观察；已提交不能取消 |
| 缺 Offset、低于起点/高于终点、UUID/分区变化 | 原始 Kafka/目标身份明确拒绝，B2与冻结保留 |
| lag0但隔离、缺内容、CK同版本冲突、未知writer | 完整对账或状态证明拒绝，不以lag或旧报告放行 |
| 取消失败回滚 | B仍被选择；普通Resume拒绝；未知/死亡boot拒绝，健康B实际验证后显式恢复 |
| 已提交失败的显式逆向恢复 | 保留前操作与冻结，新验证和新提交B4；取消恢复的重试不打开前置操作 |
| 并发推广/回滚、重复操作、破坏反向引用 | 一个合法意图成功；重复保持一次修订；损坏审计拒绝读回 |

四次失败运行完整保留，见 [failed-attempt-index.json](../artifacts/m4-03c3/failed-attempt-index.json)。首次 Gold 断言未计新待处理积压，修正独立预期后恢复；并发夹具未重试非等待控制锁、透明代理端点配置与实际 writer 不一致，随后修正测试入口；CLI 两次意图用了相同证据文件名，改为按操作留存。没有因失败降低目标、权限、位置或排空要求。每次尝试均保留日志及当时源码清单。

## 8. 环境、命令与回归

[RUNTIME] 私有基础实例是 MySQL8.4.5:33315、Kafka4.4.0:19108（Controller19109）、ES8.17.5:19209、CK:18132（4GiB tracker）。MySQL UUID `1d137b7a-e10a-11f1-acf3-adab4f8ac84c`，Kafka cluster `3ZmDMgg1T_qwWGFTKvGnfA`。每次测试前核对这些实际进程/数据目录/端口和目标资源；测试资源为新随机 schema/Topic/index/CK，不复用演示或 B 旧轮随机资源。预算为本轮磁盘8GiB、owned RSS8GiB、最低空闲20GiB，到界立即停止。C3只用一 Broker，本轮不重复 M4-02 多副本故障专题。

以下命令只针对脚本严格识别的 C3 私有环境；不可替换为演示 DSN 或端口。每次运行的新目录保存环境身份、源码哈希、原始日志、退出码和资源采样。

```bash
python3 scripts/m403c3_suite.py focused
python3 scripts/m403c3_suite.py forward
python3 scripts/m403c3_suite.py read-control
python3 scripts/m403c3_suite.py foundations
python3 scripts/m403c3_suite.py handoff
python3 scripts/m403c3_suite.py regression
python3 scripts/m403c3_suite.py static
python3 scripts/m403c3_live.py start
python3 scripts/m403c3_live.py observe
python3 scripts/m403c3_preservation.py
python3 scripts/m403c3_package.py
```

| 范围 | 最终结果 |
| --- | --- |
| C3 | 10项专项；14拒绝子场景；12控制器真实SIGKILL边界；最终专项已通过 |
| C2 | 14项；PASS |
| C1 | 12项；PASS |
| M4-03A/B | 7/21项；PASS |
| M1–M5 | MySQL78项+syncpipe11项；PASS |
| Go race / go vet / Python unittest | PASS / PASS / 3项PASS |

测试最终路径、数量和资源上界在 [test-summary.json](../artifacts/m4-03c3/test-summary.json)，不把服务保留夹具的持续运行计为完成测试。所有真实专项使用 race 构建。333次预算采样最大owned RSS4,994,727,936字节、最大本轮磁盘7,515,995,773字节、最低空闲237,546,549,248字节，均在既定预算内。以前 M5 十万容量数据、M4-02 三Broker故障证据仅作历史复用，未声称本轮重新验证了性能或物理故障域。

维护 CLI 示例使用明确操作 ID 和前置操作引用；凭据从现有控制环境读取，不打印：

```bash
ticketpulse-control -mode=migrate-read-rollback
ticketpulse-control -mode=freeze -reason='controlled rollback'
ticketpulse-control -mode=read-begin -operation=rollback -direction=ROLLBACK \
  -rollback-of=forward -version=active_original -read-revision=2
ticketpulse-control -mode=read-switch-drain-plan -operation=rollback -api-scope=scope.json
ticketpulse-control -mode=read-validate -operation=rollback
# 操作者按已核对PID排空/停止全部受控旧API，再观察退出。
ticketpulse-control -mode=read-switch-drain-observe -operation=rollback
ticketpulse-control -mode=read-switch-commit -operation=rollback
# 操作者启动配置与当前选择一致的新API，再提供当前boot及探针凭据。
ticketpulse-control -mode=read-switch-observe-ready -operation=rollback -api-boot=<new_boot> \
  -probe-start=2026-11-01 -probe-end=2026-11-04
ticketpulse-control -mode=read-switch-resume -operation=rollback -freeze-id=<exact_freeze> \
  -api-boot=<new_boot> -probe-start=2026-11-01 -probe-end=2026-11-04
```

## 9. 保留环境、演示与历史资产

本轮完成后保留 A/B 投影、四个消费组、全部历史和共享 CDC；不实现或执行退休。最终创建的独立往返环境使用专属不可变可执行目录及常驻受控 launcher，A3 API、共享CDC、A/B四个消费者和基础实例保持运行。独立观察另行读取进程、真实HTTP、组状态与两套完整对账，保留身份在 [retained-live-summary.json](../artifacts/m4-03c3/retained-live-summary.json)。这一常驻夹具没有自动重启或自动回滚；若未来进程停止，必须重新观察，旧快照不能永久证明活着。

[RUNTIME] 保留服务为 `http://127.0.0.1:60555`，新schema为 `ticketpulse_test_5bd83f5da483461f`，Topic UUID `kiQ1YU1OTCuKu4g9cLMiLg==`。A与B各自的实际索引、CK空间和UUID随保留身份保存。原A两组、B版本化两组均有3分区和Stable单成员；分组观察在不同时间执行，周期源标记会继续推进，不能把这些异时截图当同一个Kafka结束边界。独立两套Reconcile仍为12工单/51规范历史、差异0、冲突0，API统计窗口Gold保持，当前积压按两张新待处理工单变为7。原ES freshness的caught_up仍按旧合同为null，没有为回滚伪造true。

保留API的真实就绪与恢复证明另存 [live-api-ready.json](../artifacts/m4-03c3/live-api-ready.json)、[live-api-resumed.json](../artifacts/m4-03c3/live-api-resumed.json)。可以重复只读运行 `python3 scripts/m403c3_live.py observe`；不要修改或重编保留可执行目录，不用历史就绪记录代替重新观察。

[RUNTIME] 演示数据按既有 M4 基线只读核对：MySQL逐字段、ES字段/外部版本/UUID、CK全部物理事实；011–015未安装，原端口可访问。原进程与C2记录身份一致，见 [demo-process-preservation.json](../artifacts/m4-03c3/demo-process-preservation.json)；源检查点和旧组可因周期标记自然前进，但无重置，见 [demo-progress-preservation.json](../artifacts/m4-03c3/demo-progress-preservation.json)。

9,334个历史证据文件、3,403,253,488字节按基线SHA-256逐文件重验。报告、所有旧脚本及资产也在完整源码基线中核对。历史文件未清理或改写，结果见 [previous-evidence-preservation.json](../artifacts/m4-03c3/previous-evidence-preservation.json)。测试资源与进程保留不等于生产部署。

[CODE][RUNTIME] 完整 Diff 覆盖未跟踪仓库的实际前后字节，旧/新变更字节另存source/original及source/final。将基线展开到新临时目录，检查并应用Diff，逐文件与最终源码比较；再核对源码归档还原一致。结果在 [diff-verification.json](../artifacts/m4-03c3/diff-verification.json)、[full.diff](../artifacts/m4-03c3/full.diff)、[final-source.tar.gz](../artifacts/m4-03c3/final-source.tar.gz)、[artifact-manifest.json](../artifacts/m4-03c3/artifact-manifest.json)。没有暂存、提交、推送。

## 10. 预定目标与剩余边界

[INFER] 有限 M4 目标是：独立重建、准确增量接续、完整对账、进程级Kafka故障恢复、受控推广和显式回滚。C3真实专项与相关回归全部通过、原资产保留和完整Diff核验成立，已达到上述预定验收目标；生产高可用仍有下列未验证范围和单点。

[UNKNOWN] 跨主机进程和网络分区、任意自动重启器或未登记writer、ES/CK请求级fencing、十万规模冻结证明时间、Linux运行、投影退休均未在本轮验证。MySQL、CDC、ES、CK、API及单机磁盘/网络仍是生产单点；M4-02仅保留原PROCESS_FAILOVER_VERIFIED范围。

COMMITTED前的失败没有自动逃生：若目的A不健康，操作者可以安全取消PLANNED，并从真实B进程、当前权限、原始Offset和完整Reconcile确认B健康后显式恢复。COMMITTED后的恢复同样由操作者发起新操作或修复A API。取消后恢复的健康检查不生成新的切换标记，因为没有提交新读选择；正式回滚仍强制本轮标记证明。

未来关闭回滚窗口要另行决定：确认不再需要旧投影、记录其最后有效身份和位置、持久标明回滚已关闭，并受控停止/退休。停组、Kafka保留期丢失、身份变化或不能完成当前完整证明时，本协议拒绝回滚；不能通过重置Offset恢复许可。本轮两套均保留。

| 最终问题 | 证据结论 |
| --- | --- |
| B→A是否真实成功 | [RUNTIME] 主专项与最终保留环境均真实完成，最终M4-03C3_PASS |
| A是否获得新的完整证明 | [RUNTIME] 本次新冻结、精确标记、原始FetchOffsets、进程/组、完整对账 |
| 回滚失败是否安全 | [CODE][RUNTIME] 提交前保持B与冻结；取消后重验B；提交后受控恢复A或显式反向操作 |
| 旧游标是否复活 | [RUNTIME] A1/B2在A3为409，新A3分页正常，真正过期400 |
| Offset是否正确 | [RUNTIME] 主流程四组单调，固定接续向量与共享CDC不变；控制代码无Offset重置 |
| 自动降级 | [CODE] 无自动回滚、Offsetfallback许可、自动Resume；错误明确拒绝 |
| 同机保证 | [INFER] 受控进程排空、旧writer观测及launcher核验仅限macOS合作同机范围 |
| M4预定验收目标 | [RUNTIME][INFER] 预定有限恢复与受控切换目标满足；未宣布生产HA |

## 11. 最短源码定位

下表行号来自最终live repository，文件SHA-256与完整符号表在final-source-facts中，不能用旧报告行号定位。

| 机制 | 文件 / symbol / 行号 |
| --- | --- |
| 015完整安装及连续读历史 | [read_rollback.go](../internal/projection/read_rollback.go#L18)，ReadRollbackInstalled:18；EnsureReadRollbackSchema:39；readSelectionChain:71 |
| 反向Begin与锁/CAS | [read_reverse.go](../internal/syncpipe/read_reverse.go#L27)，beginReadSwitch:27；[read_switch_control.go](../internal/syncpipe/read_switch_control.go#L78)，BeginReadSwitch:78 |
| 取消与精确源标记 | [read_switch_control.go](../internal/syncpipe/read_switch_control.go#L145)，CancelPlannedReadSwitch:145；ensureReadMarker:255 |
| 新鲜证明与真实分区标记 | [read_switch_control.go](../internal/syncpipe/read_switch_control.go#L349)，ValidateReadSwitchDestination:349；waitReadMarker:442；proveReadDestination:528 |
| 原始提交与真实writer | [read_projection.go](../internal/syncpipe/read_projection.go#L147)，RawOffsetProgress:147；ObserveWriter:184；sameReadWriter:280 |
| legacy实际进程 | [read_legacy_process.go](../internal/syncpipe/read_legacy_process.go#L44)，RegisterLegacyObservation:44；Membership:120；LegacyProcess:141 |
| 原子选择提交 | [read_switch_commit.go](../internal/syncpipe/read_switch_commit.go#L28)，CommitReadSelection:28 |
| API枚举与实际退出 | [read_api_drain.go](../internal/syncpipe/read_api_drain.go#L59)，ConfigureReadDrain:59；checkAPIDrained:185；ObserveReadDrain:242 |
| A/B读解析、状态与游标 | [read_api_selection.go](../internal/syncpipe/read_api_selection.go#L19)，ResolveReadSelection:19；CursorTarget:43；CheckReadSelection:46；SearchFreshness:96；AnalyticsStatus:113 |
| 实际API就绪与恢复 | [read_api_ready.go](../internal/syncpipe/read_api_ready.go#L85)，checkReadReadyMode:85；ObserveAPIReady:342；ResumeReadSwitch:396 |
| 取消后健康恢复 | [read_cancel_resume.go](../internal/syncpipe/read_cancel_resume.go#L16)，ResumeCancelledReadSwitch:16 |
| 读准入与普通Resume守卫 | [projection/read_api.go](../internal/projection/read_api.go#L110)，CheckReadAdmission:110；CheckAPI:159；[read_control.go](../internal/projection/read_control.go#L304)，CheckReadResume:304 |

最终验收记录：[closeout.json](../artifacts/m4-03c3/closeout.json)。
