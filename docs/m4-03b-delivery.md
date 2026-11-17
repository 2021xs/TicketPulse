# TicketPulse M4-03B：候选投影的消费位置交接

M4-03B 在完全独立的 MySQL、Kafka 4.4、ES 和 CK 环境通过验收。候选从 M4 的分区位置接入两个新的标准 Kafka 消费组，完成新源标记确认、全量对账、故障重放和显式恢复业务。交接专项 21 项、M4-03A 基础专项 7 项、相关 M1–M5 回归 89 项通过，Go race、vet 和 Python 脚本测试通过。最终机器可核验结果见 [test-summary.json](../artifacts/m4-03b/test-summary.json) 和 [closeout.json](../artifacts/m4-03b/closeout.json)。

这个 PASS 的范围是候选消费接续。PREPARED 候选保持 PREPARED，API 仍读取原 ACTIVE；本轮没有读目标推广、ES alias 切换、CK 查询空间切换或旧消费组 Offset 迁移。写入者保护只针对同机、同一 MySQL 元数据库和参与合同的程序；ES/CK 尚没有请求级 fencing，不能据此宣称跨主机高可用。

文中 [CODE] 表示最终源码，[RUNTIME] 表示本轮保存的真实隔离执行结果，[INFER] 表示由实现推得的边界，[UNKNOWN] 表示尚未执行或不能据现有证据证明的能力。历史 M4/M4-02 报告只作为导航和明确标注的历史证据。

**源码版本与实际改动**

实施前在 feat/m1-01-ticket-loop 分支，没有有效 HEAD，全部文件未跟踪、暂存区为空。2026-11-14T08:42:15.213062Z 保存了 246 个源码、文档和配置文件的内容及哈希，核对为 M4-03A 的最终基线。没有用 git diff 为空推断源码没有变化。原始内容见 [baseline-source-manifest.json](../artifacts/m4-03b/baseline-source-manifest.json)、[pre-m4-03b-source.tar.gz](../artifacts/m4-03b/pre-m4-03b-source.tar.gz) 和 [baseline-git-status.txt](../artifacts/m4-03b/baseline-git-status.txt)。

最终新增 16 个、修改 20 个文件，没有删除源码。完整文件清单、前后大小和 SHA-256 见 [file-manifest.json](../artifacts/m4-03b/file-manifest.json)。下表覆盖全部 36 个改动文件；M4-03A/M4/M5 的历史交付文档与实验目录保留。

| 文件 | 本轮职责与影响 |
| --- | --- |
| internal/projection/handoff.go | 交接模型、合同校验、逐分区确认读取、012 显式安装入口 |
| internal/projection/writer.go | 物理目标锁、持久 owner/epoch、进程准入、归属检查与封存转换 |
| internal/projection/handoff_test.go | 合同不完整／错配、动态状态与游标指纹、未知 writer 拒绝的单测 |
| internal/projection/gate.go | Resume 拒绝未完成交接；已安装 012 后元数据缺失也拒绝降级 |
| db/migrations/012_projection_handoff.sql、db/embed.go | 七张增量控制表及 Go embed；原表与业务数据不改写 |
| internal/syncpipe/handoff_control.go | Begin、Pin、离线 CommitOffsets 初始化、消费授权、恢复检查 |
| internal/syncpipe/target_consumer.go | 候选身份、组、保留范围、单写入者与 Kafka 生命周期检查 |
| internal/syncpipe/target_store.go | 按版本／组件隔离的 delivery、heartbeat、watermark、recovery |
| internal/syncpipe/target_status.go | 候选 freshness/status/diagnostics 与新标记后的完整 Verify |
| internal/syncpipe/rebuild_writer.go | M4 写入准入、目标锁及封存后拒绝 |
| internal/syncpipe/rebuild.go、rebuild_check.go、rebuild_tail.go | 操作局部锁状态；Run/Check/Follow 写入前及位置保存前检查；复用内部 checkLocked |
| internal/syncpipe/queue.go | 候选固定 Group ID、AtCommitted、提交前归属检查及真实协议观察点 |
| internal/syncpipe/consumer.go、analytics_consumer.go | 真实客户端目标核验、候选 owner 准入；保留原投影／进度／提交顺序 |
| internal/syncpipe/store.go、watermark.go | 根据 VersionID 分流状态；CDC 检查点仍共享，legacy 默认仍使用原表 |
| internal/syncpipe/analytics_status.go、status.go | 候选 CK 状态分流；拒绝用 legacy Search status 描述候选 |
| cmd/ticketpulse-control/main.go | 012 迁移及交接维护命令；没有推广命令或 HTTP 控制入口 |
| cmd/ticketpulse-sync/main.go、handoff.go | -handoff 启动单组件候选标准消费者及只读状态命令 |
| internal/mysqlstore/m403b_handoff_integration_test.go | 21 项真实隔离专项及子进程故障驱动 |
| internal/mysqlstore/integration_test.go、m403a_control_integration_test.go | 新隔离端点的严格准入；原基础测试可在 m4-03b 重跑 |
| internal/syncpipe/reconcile_integration_test.go | 新隔离 MySQL 的回归准入 |
| scripts/m403b_environment.py、m403b_suite.py | 私有实例身份核验、资源预算、唯一执行目录、构建及测试 |
| scripts/m403b_preservation.py、m403b_package.py | 只读演示核验、历史哈希核验、完整 Diff 与逐字节还原验证 |
| README.md、docs/design.md、docs/api.md、本报告 | 维护入口、协议、公开行为与验收边界说明 |

本轮没有修改 internal/search 的筛选、MySQL 授权、排序、lookahead、2000 候选预算，亦没有修改 CK 统计 SQL、规范事实或 M4 原有源标记格式。

**从当前源码复用的机制**

| [CODE] 文件与 symbol | 准确位置 | 复用方式 |
| --- | --- | --- |
| [projection/gate.go](../internal/projection/gate.go)，AcquireBusinessWrite / Freeze / LockFrozen | 第 37 / 110 / 80 行 | 业务事务持有 FOR SHARE；冻结 FOR UPDATE 排空；交接持有同一冻结行保护 |
| [projection/store.go](../internal/projection/store.go)，Fingerprint / CursorTarget | 第 38 / 43 行 | 不可变 Contract 仍是目标指纹，handoff 的动态阶段和 owner 不进入指纹 |
| [projection_control.go](../internal/syncpipe/projection_control.go)，identities / Prepare / retained | 第 26 / 218 / 195 行 | 核对源 UUID/schema、Topic UUID/分区和 ES/CK UUID；复用候选准备及日志范围检查 |
| [rebuild_check.go](../internal/syncpipe/rebuild_check.go)，Check / checkLocked | 第 21 / 35 行 | 内部完整对账允许最终控制器连续持有原任务和目标锁 |
| [rebuild_tail.go](../internal/syncpipe/rebuild_tail.go)，tailClient / barrier / catchup / Follow | 第 17 / 57 / 83 / 166 行 | M4 手动分区位置和原始源标记；实际 next_offset 为最后处理记录的 Offset+1 |
| [queue.go](../internal/syncpipe/queue.go)，PublishBarrier / ConsumerClient / Consume | 第 266 / 302 / 397 行 | 每分区标记、原分区顺序和同步提交；候选增加 AtCommitted 和写入者检查 |
| [consumer.go](../internal/syncpipe/consumer.go)，Consumer.Run | 第 25 行 | ES 严格 external version，成功或持久隔离之后保存状态再提交 |
| [analytics_consumer.go](../internal/syncpipe/analytics_consumer.go)，AnalysisProjection / CKConsumer.Run | 第 29 / 73 行 | CK 先写不可变事实再写当前状态，保留 M3 规范去重口径 |

franz-go 保持 v1.20.1，kadm 保持 v1.17.0，kmsg 保持 v1.12.0；没有升级客户端。[CODE] kadm.CommitOffsets 构造默认 OffsetCommitRequest、提交传入 At，并将每个分区错误放入 OffsetResponses；顶层 error 只覆盖请求失败。客户端源码位置已核对，本轮用真实 Broker 响应另外验证，项目依赖见 [go.mod](../go.mod)。

[RUNTIME，历史] M4-02 的三进程 Kafka 4.4 副本／Leader 切换证据仍保存在 [matrix-summary.json](../artifacts/m4-02/matrix-summary.json)，状态为 PROCESS_FAILOVER_VERIFIED。本轮没有重新执行三 Broker 故障，不能将该历史结果扩大成新交接协议在多 Broker 故障期间也已验证。

**增量结构、交接合同和阶段**

[CODE] [012_projection_handoff.sql](../db/migrations/012_projection_handoff.sql) 只建下列七张表。已有 M2/M3/M4 同步结构是前提，安装入口先补 011，再显式执行 012；不会在普通应用或 legacy 消费者启动时自动迁移。部分 DDL 安装失败保留已建表，重新执行 CREATE TABLE IF NOT EXISTS 补齐；部署时仍须核验实际结构，不应把重复执行当成任意旧 DDL 都被纠正。

| 表 | 持久内容与唯一归属 |
| --- | --- |
| sync_projection_handoffs | handoff/version/job/freeze、合同摘要、最终报告和源标记、pinned 向量、固定两组 ID、阶段/revision/error；版本及 M4 job 各只属于一个交接 |
| sync_projection_handoff_partitions | handoff × es/ck × partition 的初始提交确认、读回时间、验证源标记 Offset |
| sync_projection_target_owners | component/endpoint/namespace 的物理目标哈希、owner kind/ID/epoch、version/handoff、当前 worker 的 host/PID/token/锁连接 |
| sync_projection_handoff_log | 提交意图、实际 Kafka 返回、读回、阶段转换及时间；独立日志不对被长事务锁住的 handoff 行建立 FK |
| sync_projection_deliveries | version × component × Topic/Partition/Offset 的处置、重试、隔离消息、恢复位置 |
| sync_projection_worker_state | version × component 的 Group ID、owner epoch、心跳、状态、错误、Kafka member/generation |
| sync_projection_watermarks | version × component × partition 的已投影标记及消息位置 |

源 MySQL、Topic、ES 和 CK 身份通过 version_id 与 contract_sha256 引用 [Handoff 模型](../internal/projection/handoff.go) 第 25 行的不可变版本。没有另复制一套可漂移的端点配置。M4-03A Contract 的 NextESWriter/NextCKWriter 保留原准备时的描述；本轮的真实新组、owner 和进度归属由独立 handoff 记录描述，不回写旧 Contract，也不误用旧提示字段启动候选。

原 ACTIVE delivery、heartbeat、watermark、bootstrap 和历史恢复记录全部保留；CDC 源检查点及源水位仍共享。HandoffStore.Get 只读输出会隐去 worker token。012 安装在原 control log 留安装记录，HandoffInstalled 第 168 行复核该记录及控制表。安装后交接或 owner 元数据消失，Resume、M4 准入和普通消费者均保守返回 HANDOFF_METADATA_UNAVAILABLE，不能退回 legacy 路径；从未安装012的环境继续兼容。

建议的四个交接阶段前增加一个必要的 DRAINING。它记录“停止 follower 并准备最终封存”的持久意图，解决 begin 成功后 CLI 崩溃、follower 尚未退出时的安全边界。

| 阶段 | 持久事实与成功证明 | 故障后的安全恢复 |
| --- | --- | --- |
| DRAINING | 已绑定 PREPARED、job、确切 freeze ID/revision；普通 M4 Run/Check/Follow 不再准入 | 等待现有 follower 结束；同 ID 重试 Begin/Pin。无法证明旧 writer 停止时保持冻结 |
| PINNED | 任务锁、两目标锁、冻结保护下的新完整报告；全分区同一最终标记；实际 next_offset 与 sealed owner 一笔提交 | 读取持久阶段确定封存是否提交；允许在新组没有消费者时初始化同一向量。不能让 M4 重新写候选 |
| OFFSETS_READY | ES/CK 两组所有分区已逐个检查返回并从原始 FetchOffsets 读回准确初始位置 | 只读复核相同向量后消费授权；已确认提交消失则拒绝，不静默补初始位置 |
| CONSUMING | 两个 sealed owner 原子转为固定 consumer_group，阶段先于工作进程启动持久提交 | 初始化永久禁用；重启使用当前 committed offset。未知 writer、缺失位置或身份变化均拒绝 |
| VERIFIED | 新标记被新组所有分区投影及提交越过；无未完成/隔离；新完整对账一致；有效 owner 和单成员 Stable 组 | 可显式 Resume。报告是该时刻接续证明，重试 Verify 只核对身份并返回历史结果，不自动更新为当前完整性证明 |

没有 ABORT、自动抢占、自动回滚或 PREPARED→ACTIVE 转换。如果 Topic、保留范围或持久元数据损坏，未完成交接会保持冻结，直到明确的人工恢复方案完成。不能通过手工改阶段或重置 Offset 绕过证明。

**最终对账到位置封存**

[CODE] HandoffControl.Begin 第 93 行首先验证 PREPARED 和当前源／Topic／目标身份，在冻结锁内持久记录 DRAINING。它要求两物理目标已有该 M4 job 的 owner 行。因此必须在 012 安装后用新版 M4 Run/Check/Follow 建立写入者 provenance；仅有旧报告不能开始交接。正在运行的新版 follower 每次循环／记录都检查 handoffAdmission，即使没有新数据也会观察到停止意图并退出。

HandoffControl.Pin 第 150 行取得原 M4 job lock 和两个物理目标 lease，随后持续持有冻结行和 handoff 行事务锁。控制器的内部 checkLocked 在有限期限内按原 M4 源标记追平并完整对账。完成后重新读取 sync_rebuild_offsets：每个分区的 barrier_id 必须与 job 的最终标记一致，next_offset 必须等于实际持久位置；再核对 Kafka 保留范围及全部目标 UUID。

PINNED 提交同时保存最终报告、报告哈希、标记、分区位置及两个 sealed owner。这段过程没有先放开目标保护再执行封存的窗口。公开 M4 Run/Check/Follow 之后均拒绝同一目标；另一个 job 或省略 -handoff 的 legacy 消费者也不能凭别名越过物理目标 owner。封存只更新候选的控制记录，没有修改 CDC 检查点。

[RUNTIME] [drained-and-pinned.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BFollowerDrainsPersistentIntent/drained-and-pinned.json) 保存现有 follower 收到持久意图后的排空与最终封存。SIGKILL 专项在真实独立子进程调用生产 HandoffControl 的观察边界前后终止进程；[故障目录](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BSealAndInitialCommitSIGKILL/) 保存 before_pin_commit、after_pin_commit、after_initial_commit、after_offsets_ready_commit 的边界输出和死亡后持久记录。前者恢复为 DRAINING，后者为 PINNED，恢复逻辑依据 MySQL 已提交事实。确定边界的子进程驱动不是 mock Kafka/MySQL；维护二进制完整命令链另有独立验证。

**逐分区 Kafka 初始化与标准消费者接续**

[CODE] TargetGroup 第 57 行确定 `<topic>.es.target.<version_id>` 和 `<topic>.ck.target.<version_id>`。它们与原 `<topic>.es.v1`、`<topic>.ck.v1` 不同，创建后不可换组。Initialize 第 304 行使用 schema/handoff 范围的操作锁、冻结锁、交接行锁及组空闲检查，只在 PINNED／OFFSETS_READY 处理。

每个分区实际执行顺序是：保存 offset_commit_intent → kadm.CommitOffsets → 检查请求错误及每分区响应 → 原始 FetchOffsets → 精确比较 → 保存逐分区确认。请求 deadline 为 8 秒，generation=-1、member_id 为空、leader_epoch=-1，metadata 为 handoff ID 与 version ID。Topic UUID 是提交前后身份检查及日志字段，Kafka OffsetCommit 本身按 Topic 名和分区提交。

对部分成功或响应未知，当前尝试不报告 OFFSETS_READY。重新执行同一任务先读取 Broker：已经保存的位置必须与 pinned 一致，且能在持久意图日志中找到本交接的提交来源、metadata 也一致；只补齐尚缺的相同 pinned 位置。新组已有无关位置、已有成员、分区响应缺失或读回不等都拒绝。不能用 Queue.Progress 的起点兜底把新组误判为初始化完成。

[RUNTIME] 主链实际三分区起点与验证后位置如下。pinned 表示下一条应读的位置，离线提交直接使用该数值。消息 Offset+1 的转换只发生在“处理一条记录之后保存 next/提交”的原路径，没有在交接初始化再加一。

| 分区 | M4 pinned next_offset | ES 初始 committed | CK 初始 committed | 新验证标记的消息 Offset | 两组越过验证标记后的 committed |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 34 | 34 | 34 | 35 | 36 |
| 1 | 28 | 28 | 28 | 29 | 30 |
| 2 | 22 | 22 | 22 | 23 | 24 |

逐字实际请求、kadm 原始分区响应、读回和时间见 [protocol.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BOfflineNewGroupsExactPositionsAndGold/protocol.json)；六个持久确认见 [handoff.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BOfflineNewGroupsExactPositionsAndGold/handoff.json)。

另外，真实空 Topic 的三个分区 start=end=0，新空组离线提交和原始读回均为 0；AtCommitted 没有跳过第一条，随后实际读到 Offset=0，见 [empty-group.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BEmptyPartitionsOfflineCommitAtLogEnd/empty-group.json)。这是 Kafka 的空分区原语验证；完整 M4 pin 会发布全分区源标记，因此 pin 完成时各分区至少已含标记。

提交响应截留测试把真实 OffsetCommit 发给独立 Broker，读出真实响应后不给客户端，得到 i/o timeout。正常客户端原始 FetchOffsets 显示第一个分区已提交，交接仍为 PINNED；重试补齐后完成，见 [real-timeout-before-retry.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BRealKafkaCommitResponseTimeout/real-timeout-before-retry.json)。这证明超时不能被解释为“Broker 肯定未提交”。

真实非空组的离线 CommitOffsets 返回 top-level error=nil，但三个分区均为 UNKNOWN_MEMBER_ID，见 [real-partition-errors.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BKafkaInitialPartitionErrorsAndGroupNotEmpty/real-partition-errors.json)。正式初始化路径先拒绝该非空组；独立测试直接调用 kadm 取得协议错误，验证 Error() 的必要性。

OpenConsumption 第 458 行重新检查空组、准确位置、保留范围及 sealed owner，确认两目标没有活锁／未清理 token，才在同一事务写 CONSUMING 和新的 owner kind/ID/epoch。它先提交授权，再启动消费者；即使进程在启动前被杀，后续也只能按 committed 恢复。

ConsumerTarget、Acquire 和 committedProgress 分别位于 [target_consumer.go](../internal/syncpipe/target_consumer.go) 第 36、105、74 行。候选每次启动检查当前 Topic UUID、完整分区集合、源 UUID、实际 ES/CK 客户端及表 UUID，每个分区的 committed 必须存在且满足 pinned ≤ committed、log_start ≤ committed ≤ log_end。实际 client 使用 AtCommitted；缺失位置、保留范围失效、身份查询失败都明确拒绝，没有 earliest/latest 回退。OFFSETS_READY 之后的重试只读证明；CONSUMING/VERIFIED 永久拒绝初始化。

**写入者保护的真实保证**

[CODE] AcquireWriter 位于 [writer.go](../internal/projection/writer.go) 第 85 行。目标 key 使用组件、endpoint 和 index/database 的 SHA-256，而非 job/version 别名。每个组件有一个专用 MySQL named lock 连接，同时有持久 owner/epoch/token；所有合作程序必须使用同一源元数据库。ES、CK 可以分别在两个进程中运行，每个组件至多一个写入进程。

WriterLease.Check 第 182 行串行完成锁连接查询和 Scan，再检查持久 owner、epoch、token、锁连接及 version/handoff 链接。写入前、进度保存前和 Kafka CommitRecords 前都检查。目标连接失败就结束该写入进程的后续处理；候选启动方式不自动无限重启或抢占。StateStore.writerCheck 第 11 行还检查 lease 的组件、Group ID 和 VersionID，ES lease 不能被拿去写 CK 或别的版本进度；KeepAlive 同样拒绝不同组件，续期SQL也带版本、组件、Group ID和epoch。

新进程不能只看 named lock 空闲：旧 owner token 尚在时，只有已明确死亡的同机 PID 才能接续；远端、未知、仍存活或不可证明死亡的旧进程拒绝接管。干净释放会清理 token；崩溃留下 token，读取 PID 后确认死亡再增长 epoch。PID 复用导致保守拒绝。

[RUNTIME] 真实重复 ticketpulse-sync 进程在写入前被拒绝。测试 SIGSTOP 旧进程、KILL 其独立 MySQL 锁连接，再启动竞争者；竞争者仍因旧 PID 存活被拒绝。恢复旧进程后它发现锁丢失退出，确认死亡后新进程才取得更高 epoch，见 [writer-ownership.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BDuplicateCandidateProcessRejectedAndLockLoss/writer-ownership.json)。Kafka 重平衡测试另用仅加入组、不写后端、不提交位置的 Broker peer，触发真实 assigned/revoked；它不能代表支持多个候选投影写入成员，见 [rebalance.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BNormalConsumerRebalancePreservesPositions/rebalance.json)。

[INFER] owner epoch 没有作为 ES/CK 后端拒绝旧请求的 fencing token。锁连接失效之前后端已接受的请求可能迟到完成，检查与后端请求间也不是跨系统原子事务。首版因此只在已证明旧合作 writer 停止时接管；无法证明时继续冻结或拒绝接管。任意直连 SQL、直接写 ES/CK、未参与合同的旧二进制、同物理后端的不同 DNS 别名及不同元数据库不在互斥保证内。部署需统一端点和元数据库，并排除这些写入者。不能将本轮同机验证扩大为严格网络分区 fencing。

**版本隔离的状态与新验证标记**

[CODE] VersionID 为空保持原表和原默认消费组；非空时 delivery、heartbeat、watermark、recovery 的键均带版本和组件。投影处理保持先后顺序：ES 写入／CK fact+current 成功 → 本版本持久 delivery/水位/心跳 → 再检查 owner → CommitRecords。暂时失败不越过该分区记录，终态隔离持久化后可推进消费，但 VERIFY 会拒绝未解决隔离。

TargetStatus.AnalyticsStatus / SearchFreshness / Diagnostics 位于 [target_status.go](../internal/syncpipe/target_status.go) 第 127 / 130 / 143 行，只读候选的新组、owner、心跳和水位。caught_up 需要本版本的活锁／当前 epoch、近期心跳和源水位、同一标记的全部分区确认、零 lag 及零未完成记录；不复制旧 CK caught_up。候选读器可以供后续受管理 Search 使用，本轮没有接入 ACTIVE Search HTTP。原 mysqlstore.SearchFreshness 及当前看板状态继续描述原 ACTIVE。

[RUNTIME] [recovery-isolation.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BVersionRecoveryNamespaceAndOldState/recovery-isolation.json) 中同一物理记录，候选 original 变为 recovered，旧 ACTIVE original 仍为 isolated；候选失败心跳不改变旧状态。该定向测试为固定原行停止了它自己夹具中的旧 ES 消费者，没有停止演示消费者。另一个并行工作测试让旧 ACTIVE 和候选各自消费同一无效消息，两个候选隔离行使候选 freshness 为 degraded，旧 freshness 仍从原表读取，见 [independent-failure-state.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BVersionStateIsolationAndFailureRefusesVerify/independent-failure-state.json)。

Verify 位于第 165 行。它在冻结保护下提交新的 MySQL 源标记，CDC 将其按原协议发布至每个分区。等待两组的相同新标记 delivery=applied 且 committed>marker_offset，无未完成处置，并重新全量比较 MySQL/ES/CK。对账后再核对目标身份、有效 owner/current epoch、近期 running 心跳和一个 Stable 成员，最后原子保存 VERIFIED。任何失败保留 CONSUMING 和恢复错误，业务不能提前 Resume。

本次主链最终标记为 `6957c10d9d59f20489e2d970a5605e17`，标准组验证使用新的 `56c2e1c57a957a064982c9e2d9345559`；两组都越过后者。最终报告摘要为 `d221ad22b56d7708729409d4f320c767013c0de082b0e4422ec328efd00a2d2a`，验证报告摘要为 `51f471bdb42087498511bc7875e8bccd08cc13ffcd7f6a3904fd12e6d21d620a`。原始完整报告和确认向量均保存在主链 handoff 文件，不能只用摘要或旧 consistent 字符串宣布通过。

**隔离环境与真实执行**

[RUNTIME] 新进程、配置和数据目录全部位于 `.local/m4-03b/`，没有复用演示实例。每次运行先检查 marker、实际进程命令／数据路径、MySQL UUID/port/datadir、ES cluster、CK config 和 Kafka cluster/quorum，再创建随机 `ticketpulse_test_*` 源库、Topic、组、候选索引及 CK database。套件清除继承的 TICKETPULSE 配置并加载该独立环境；原凭据仅保存在忽略的私有运行目录，没有打入日志或源码交付。

| 组件 | 本轮真实环境 |
| --- | --- |
| MySQL 8.4.5 | 33312，UUID 268fd452-e009-11f1-9f2c-6892a37a7d02，独立 Unix socket/datadir |
| Kafka 4.4.0 KRaft | Broker 19102，Controller 19103，cluster K1z4a1EaQuCmr9mGtks4DA，单进程、副本1 |
| ES 8.17.5 | HTTP 19206，transport 19306，cluster ticketpulse-m403b |
| ClickHouse | HTTP 18129，native 19007，独立配置，内存 tracker 4 GiB |
| 测试 API／工作进程 | 专项新建实例与临时端口，使用独立 DSN；独立二进制在 .local/m4-03b/bin/ |

身份原文和预算见 [environment.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/environment.json)。预算为私有 RSS≤8 GiB、私有磁盘≤8 GiB、空闲磁盘≥20 GiB；超限脚本立即停止该测试命令。正式专项和相关回归观测的峰值私有 RSS 约 2.44 GiB、私有磁盘约 1.82 GiB，最小空闲磁盘约 233 GiB，均未超预算。这里的 RSS 包含四个私有服务；不是性能容量结论。

所有必须执行的场景都由真实服务完成，详见 [fault-matrix.json](../artifacts/m4-03b/fault-matrix.json)。单元检查不计入下表的真实专项通过数。

| 用户场景 | 实际注入与观测 | 结果 |
| --- | --- | --- |
| 1 三分区不同起点 | 两组准确提交／读回 34、28、22，标准消费者接续并越过新标记 | PASS |
| 2 空分区／log_end | 空组逐分区提交0、读回0，AtCommitted 读取后来第一条 Offset0 | PASS |
| 3 M4 后端成功、位置失败 | SQL trigger 拒绝 next_offset；CK 物理行已增加，重放后 Gold／全量一致 | PASS |
| 4 封存前后 SIGKILL | 控制子进程在持久提交前后被 SIGKILL；DRAINING 与 PINNED 正确区分、恢复 | PASS |
| 5 初始化部分成功 | 第一分区实际提交后中止；保留 PINNED、相同向量补齐 | PASS |
| 6 CommitOffsets 超时／未知 | 真实 Broker 响应被私有 socket 截留，读回已提交位置，重试正确 | PASS |
| 7 初始化后、消费前 SIGKILL | OFFSETS_READY 提交后的控制子进程被 SIGKILL；后续只复核原初始化 | PASS |
| 8 消费开始后 SIGKILL | 真正 ticketpulse-sync worker 被 SIGKILL，按当前提交恢复，不重置 | PASS |
| 9 ES/CK 后端成功、进度失败 | 两组件分别注入 terminal delivery SQL 失败；不越过未保存记录，重放恢复 | PASS |
| 10 Kafka Offset Commit 失败 | 真实 coordinator 移除 member，CommitRecords 返回 UNKNOWN_MEMBER_ID，重放 | PASS |
| 11 Rebalance | Broker peer 加入／退出，真实回调和位置连续，后端只有一个候选 writer | PASS |
| 12 重复进程 | 写入前拒绝；旧 PID 存活且锁连接断开仍拒绝，死亡后 epoch 增长 | PASS |
| 13 Topic UUID／分区集合 | 同名 Topic 重建、增加第四分区分别拒绝 | PASS |
| 14 缺失／越界位置 | DeleteOffsets、>log_end 的提交被拒绝；真实 AtCommitted 错误；另覆盖 log_start>pinned | PASS |
| 15 ACTIVE／候选状态隔离 | 相同物理记录独立处置／恢复；不同组件／版本的 lease 不能写进度 | PASS |
| 16 未 VERIFIED 的 Resume | PINNED／OFFSETS_READY／CONSUMING、候选隔离、元数据缺失均拒绝 | PASS |
| 17 VERIFIED 后 Resume | 显式解除冻结，真实 HTTP 评论新增v11，原 ACTIVE／候选两端都追平 | PASS |
| 18 PRD Gold／完整对账 | 新组前后统计与内容全量一致；新组验证后新增业务仍 consistent | PASS |
| 补充 CK UUID／保留边界 | 候选 CK 状态表替换 UUID；PINNED、OFFSETS_READY、CONSUMING 真实日志起点失效均拒绝 | PASS |
| 补充维护 CLI | 二进制执行完整交接链、候选状态查询、早期 Resume 拒绝和最终 Resume | PASS |

21 个顶层专项实际通过；另一个顶层 TestM403BHandoffCrashChild 仅在父运行中标为 child-only SKIP，它在 SIGKILL 子进程内执行，没有把此 SKIP 算成通过。[integration.log](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/integration.log) 的包执行时间为 163.387 秒。

**Gold、完整对账及回归**

原 PRD 十工单预期仍来自 [tests/analytics/prd_gold.json](../tests/analytics/prd_gold.json)。主链全量为10工单、46历史，新增10、首次解决6、平均首次解决60600秒、当前积压5，团队和日期拆分也由原 Gold 检查。最终和标准组验证两个全量报告均 difference_count=0、source_changed=false；ES10当前文档，CK10规范当前键／46规范历史键／0冲突键。CK物理状态56行不被误当工单数。

显式 Resume 后通过原 HTTP 评论入口新增历史，两个目标均到最新v11，候选全量成为10工单／47历史，仍 consistent，见 [gold.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BOfflineNewGroupsExactPositionsAndGold/gold.json)、[target-status.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BOfflineNewGroupsExactPositionsAndGold/target-status.json) 和 [after-resume.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BOfflineNewGroupsExactPositionsAndGold/after-resume.json)。

投影后进度失败专项观察到 CK 同业务版本物理事实从1行变为2行，最终规范历史、首次解决、积压和全量内容仍正确，见 [progress-failure-and-recovery.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BStandardProjectionProgressFailureAndSIGKILLRestart/progress-failure-and-recovery.json)。因此本轮保持 at-least-once 与规范幂等，没有宣称物理 exactly-once。

| 最终执行命令 | 环境与结果 | 原始日志 |
| --- | --- | --- |
| python3 scripts/m403b_suite.py focused | 身份验证后的独立环境；go test -race -count=1，21 PASS，163.387s | [专项](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/integration.log) |
| python3 scripts/m403b_suite.py foundations | 同一独立环境；M4-03A 7 PASS，18.333s | [基础专项](../artifacts/m4-03b/runs/20261114T104744.293435Z-foundations/integration.log) |
| python3 scripts/m403b_suite.py regression | 同一独立环境；MySQL78 PASS，115.564s；syncpipe11 PASS，2.344s | [MySQL回归](../artifacts/m4-03b/runs/20261114T104453.610887Z-regression/integration.log)、[syncpipe回归](../artifacts/m4-03b/runs/20261114T104453.610887Z-regression/syncpipe-integration.log) |
| python3 scripts/m403b_suite.py static | Go race、vet、Python unittest 3项均通过 | [race](../artifacts/m4-03b/runs/20261114T104130.415829Z-static/unit-race.log)、[vet](../artifacts/m4-03b/runs/20261114T104130.415829Z-static/vet.log)、[Python](../artifacts/m4-03b/runs/20261114T104130.415829Z-static/python-tests.log) |
| python3 scripts/m403b_preservation.py | 只读原演示与历史基线比较，通过；不执行迁移、对账修复或 Offset 操作 | [preservation目录](../artifacts/m4-03b/preservation/) |
| python3 scripts/m403b_package.py | 基线+最终归档、git apply 逐字节重建、历史哈希、Git暂存状态、服务和报告链接检查 | [diff-verification.json](../artifacts/m4-03b/diff-verification.json)、[closeout.json](../artifacts/m4-03b/closeout.json) |

相关回归包含 M1 创建／所有生命周期／转派／评论／登录／当前权限，M2 实际 CDC、事务发布失败与独立组，M3 Gold 和投影部分成功，M4 CLI SIGKILL、follow、Topic 身份／保留范围／全量对账，M5-03 重复迟到冲突与首次解决／重开／转派，M5-04 成员变化和搜索游标。没有重新跑十万数据的性能矩阵或浏览器容量验收。

构建与测试使用 `./cmd/... ./internal/... ./db/... ./web/...` 明确源码目录。历史 artifacts/m4-03a/source 中保存了不完整 Go 文件快照，直接 `go test ./...` 会发现并尝试编译这些证据目录而失败；保留历史资产，使用上述实际包目录完成 race/vet。没有删除历史快照来制造全仓命令通过。

**失败尝试与纠正记录**

本轮保留所有尝试，不覆盖失败日志。正式成功之前发现的问题已经定位并修正；它们与专门注入且预期拒绝的故障分开记录。完整失败列表由 test-summary 的 failed_attempts_preserved 保存。

| 运行目录时间 | 实际问题 | 修正与最终证明 |
| --- | --- | --- |
| 090652 focused | DRAINING 的 NULL final_report 直接 Scan 到 json.RawMessage 失败 | 先接收 nullable bytes；Begin、pin SIGKILL及最终完整专项通过 |
| 090758、090943、091148 focused | 同一 named-lock sql.Conn 的并发 QueryRow/Scan 破坏连接；Verify 超时 | WriterLease mutex 包住查询到 Scan 全段；race与多分区真实专项通过 |
| 092607 focused | 新测试未使用 import，编译失败 | 移除未使用导入，保留编译失败日志 |
| 094052 focused | 状态隔离测试手工固定旧行时，仍运行的夹具旧消费者覆盖该行 | 仅该定向夹具停止旧消费者，真实双目标并行在另一专项验证 |
| 102200 focused | 新保留范围测试要求错误包含 OFFSET；实际已正确拒绝为 outside retained log | 修正测试断言接受当前既有拒绝语义；不改生产协议，完整21项复跑通过 |
| 103742 focused | 收尾定向测试复现 KeepAlive 误传组件时 ES lease 续期CK心跳 | 增加组件准入和SQL Group ID约束；保留失败日志，定向测试及最终完整验收通过 |

各目录位于 [runs/](../artifacts/m4-03b/runs/)。真实 i/o timeout、UNKNOWN_MEMBER_ID、SQL trigger失败、重复writer拒绝、UUID/保留范围拒绝属于必须观测的故障结果；相应测试成功表示这些操作确实失败且恢复合同成立。

**维护顺序及可复现命令**

重跑专项的前提是本机已有固定版本的 MySQL/ES/Kafka/CK 发行包。脚本只读复用发行包，另建 `.local/m4-03b/` 数据与端口，不调用原演示 setup/start/stop。服务创建前先检查私有端口和路径，套件创建资源前再查实际身份。以下命令在项目根执行，历史目录不会被覆盖：

```bash
python3 scripts/m403b_environment.py start
python3 scripts/m403b_environment.py verify
python3 scripts/m403b_suite.py focused
python3 scripts/m403b_suite.py foundations
python3 scripts/m403b_suite.py regression
python3 scripts/m403b_suite.py static
python3 scripts/m403b_preservation.py
python3 scripts/m403b_environment.py stop
python3 scripts/m403b_package.py
```

套件为每次运行生成唯一目录，并保存逐命令时间、exit_code、安全预算及原始日志。最终四类执行另外保存 source-at-run.json；打包时再次核验这些源码哈希与交付源码一致，避免把旧代码结果记成最终通过。临时随机库／Topic／候选在夹具结束时只清理本夹具资源；四个独立服务的目录及所有证据保留。stop 重新核验实例 PID、marker和有效命令后仅停止 m4-03b 服务，不按通用进程名杀服务。

以下是正式协议的维护示例。仅对已确认独立的源库、候选和新组使用；控制 DSN 与同步 DSN 从外部安全注入，禁止把凭据写进命令历史或日志。新组 ID 不由环境变量或命令临时覆盖。真实二进制执行的参数与输出见 [cli-commands.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BOperatorCLIFullHandoffAndCandidateStatus/cli-commands.json)。

```bash
go run ./cmd/ticketpulse-control -mode=migrate-handoff
go run ./cmd/ticketpulse-control -mode=freeze -reason='candidate handoff'
# 在012安装后用新版M4完成/复查候选，建立物理目标owner；
# 在冻结下用M4-03A prepare登记PREPARED；已有ACTIVE只读目标保持。
go run ./cmd/ticketpulse-control -mode=prepare -version=candidate_v1 -job=handoff_gold
go run ./cmd/ticketpulse-control -mode=handoff-begin -version=candidate_v1 -handoff=handoff_v1
go run ./cmd/ticketpulse-control -mode=handoff-pin -handoff=handoff_v1 -wait=2m
go run ./cmd/ticketpulse-control -mode=handoff-init -handoff=handoff_v1
go run ./cmd/ticketpulse-control -mode=handoff-status -handoff=handoff_v1
go run ./cmd/ticketpulse-control -mode=handoff-consume -handoff=handoff_v1
```

handoff-pin 尚不能取得 job/target lock 时，保持冻结，确认 follower 已按持久意图结束后重试；不能抢走锁。后续在两独立工作进程中分别执行：

```bash
go run ./cmd/ticketpulse-sync -mode=consume -handoff=handoff_v1
go run ./cmd/ticketpulse-sync -mode=ck -handoff=handoff_v1
```

这两条都必须显式配置与持久候选一致的 ES URL/index 和 CK URL/database。-handoff 与 API 的 TICKETPULSE_TARGET_VERSION 互斥，仅允许 consume、ck、status、analytics-status；候选不能执行 capture/bootstrap/cleanup/replay 或修改旧 ACTIVE。当前重放状态操作具备版本隔离，首版没有开放候选人工隔离重放 CLI。

工作进程运行后由操作者执行验证、读取状态，确认 VERIFIED，再提供本次真实 freeze_id 显式恢复：

```bash
go run ./cmd/ticketpulse-control -mode=handoff-verify -handoff=handoff_v1 -wait=2m
go run ./cmd/ticketpulse-sync -mode=status -handoff=handoff_v1
go run ./cmd/ticketpulse-sync -mode=analytics-status -handoff=handoff_v1
go run ./cmd/ticketpulse-control -mode=resume -freeze-id='<本次持久freeze_id>'
```

SIGKILL/超时之后先执行 handoff-status，依据持久阶段选择操作；CONSUMING 后不再执行 handoff-init。VERIFIED 是一次已验证的接续事实，后续服务故障仍需检查当前状态；没有自动恢复业务或自动推广。

**原环境、历史资产与完整 Diff**

[RUNTIME] 演示 MySQL 九张业务／身份表的行数和内容哈希与原 M4 基线一致：5工单、29历史、2评论等均保留。ES5个文档及 external versions 完全相同；CK全部物理事件29行、状态39行逐行一致。演示库的011四张、012七张控制表都为0。33306、19092、19200、18123、18080五个原演示端口持续可达。保存核验仅查询，不执行迁移、冻结、业务写入或原组 Offset 操作。

5,499 个先前 artifact 文件合计 3,356,395,258 字节逐一核验 SHA-256，零变更；历史交付报告也与实施前源码相同，见 [previous-evidence-preservation.json](../artifacts/m4-03b/previous-evidence-preservation.json)。未暂存、提交或推送，最终 Git 状态见 [final-git-status.txt](../artifacts/m4-03b/final-git-status.txt)。验收结束只停止重新验证身份的 m4-03b 四个私有服务，保留数据目录及证据；停止前身份和停止后端口核验见 [environment-closeout/](../artifacts/m4-03b/environment-closeout/)。

[full.diff](../artifacts/m4-03b/full.diff) 覆盖实施前246文件到最终262文件的全部源码／文档改动。[final-source.tar.gz](../artifacts/m4-03b/final-source.tar.gz)、[final-source-manifest.json](../artifacts/m4-03b/final-source-manifest.json) 和 [source/](../artifacts/m4-03b/source/) 保存最终完整源码及改动文件的前后内容。打包程序在临时目录还原原始归档，用 git apply --check、git apply 应用完整 Diff，再比较每个文件的逐字节内容；不使用 Git index，最终归档也重新打开核对全部字节。结果见 [diff-verification.json](../artifacts/m4-03b/diff-verification.json)，全部资产另有 [artifact-manifest.json](../artifacts/m4-03b/artifact-manifest.json)。

**真实验证、仅源码／单元证据及留给 M4-03C 的风险**

已真实验证：三分区精确位置与空组行为，持久排空和封存，部分／未知初始化的恢复，标准消费者 AtCommitted 接续与 SIGKILL恢复，投影后进度失败／Kafka提交失败的重放，单机重复 writer 拒绝，Topic/分区/ES/CK UUID与保留范围拒绝，版本状态隔离，新源标记及完整对账，PRD Gold、真实 HTTP 恢复写入，以及维护／候选状态CLI。原 ACTIVE 独立消费继续追平；没有把候选验证当作 API切换验证。

仅有单元或源码证据：伪造合同／不完整确认向量拒绝，handoff 动态值不改变游标指纹，远端未知 PID 保守拒绝。基础专项另真实验证了 M4-03A 的冻结并发、CLI/应用崩溃、UUID就绪和同密钥跨版本游标。未对跨主机 owner 接管或后端请求级 fencing 运行实验。

[UNKNOWN] 尚未验证：本新交接协议在多 Broker 故障期间的行为、十万规模 pin/Verify 的耗时和资源边界、跨机器／磁盘／网络故障、Linux/Docker运行、多个候选写入成员。M4-02 历史同机三 Broker结论可作为后续故障矩阵前提，不能替代这些新组合场景。单机物理故障域仍为1；MySQL、CDC、ES、CK、应用服务、单机磁盘及网络的单点仍存在。

M4-03C 仍需解决 API读目标／状态读器与已消费候选的统一推广、切换中崩溃的持久恢复、客户端旧游标失效、旧投影回滚前的追平证明，以及旧/新目标写入生命周期的控制。需要继续使用新的完整标记和对账边界，不能凭本轮 VERIFIED 历史报告直接推广。自动abort/rollback、隔离修复CLI、跨主机 fencing和多成员扩展均未实现；是否扩展需另审范围。

M4-03B 验收结论为 PASS，保证范围限于本报告已验证的候选消费交接。原 API目标及演示环境保持原状，后续推广应在 M4-03C 明确授权和独立故障验收后执行。
