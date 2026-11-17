# TicketPulse M4-03C3：B→A 正式逆向回滚复核交付

本轮在现有 macOS 同机受控协议上，重新完成了真实 A→B→A，并补验了 A 暂时落后、恢复追平后才能回切的场景。结果为 **M4-03C3_REVALIDATION_PASS**，适用范围是本次要求的受控回滚；不是生产级高可用验收。

[RUNTIME] 正常往返使用真实 MySQL、Kafka、ES、CK、API 和五个传播进程。回滚前两套投影均为11工单／49条规范历史；恢复后继续通过 HTTP 评论和创建，两套均为12工单／51条规范历史，完整对账差异与冲突为0。读修订号依次为1、2、3，旧A与B游标在A3返回409。12个控制器真实SIGKILL边界、API死亡后新boot重验、并发意图、重复提交和恢复、实际Kafka重放均通过。

本轮没有重写已有C2/C3生产代码。实施前重新核对的300个文件与上一轮C3最终清单全部匹配，因此复用已存在的正式反向协议；新增的是一个集成测试和新随机测试Topic的可选资源参数。完整M1–M5回归89项、Go race、go vet和3项Python unittest重新执行通过。此前完整C1/C2/A/B专项保留为历史证据，本轮只重跑与此次变化有关的专项，见下表。

证据标签：[CODE] 当前源码；[RUNTIME] 本轮实际执行；[INFER] 范围判断；[UNKNOWN] 未验证。所有新证据在 [本轮目录](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/)；原C1/C2/C3报告及全部失败证据保留原字节。

## 1. 基线与实际变更

[CODE] 分支为 feat/m1-01-ticket-loop，无有效HEAD，index为空。基线包含300个文件，已保存完整字节、SHA-256及模式。没有用空Git Diff判断改动；本轮Diff直接比较基线归档与最终文件。关键生产代码、011–015迁移和不可变Contract均保持基线字节。

| 文件／资产 | 职责与影响范围 |
| --- | --- |
| [新增延迟回滚集成测试](../internal/mysqlstore/m403c3_read_rollback_lag_integration_test.go) | TestM403C3TemporaryLegacyLagRequiresNewProofAndIdempotentResume，第26行。只对本次随机夹具的legacy ES文档PUT注入503；成功请求仍转发到真实ES。验证B真实业务写入、A消费落后、旧证明拒绝、新证明回滚和三次重复Commit／Resume |
| [既有Search集成夹具](../internal/mysqlstore/search_integration_test.go) | newM2，第69行开始的可选配置。只有明确启用TICKETPULSE_TEST_SMALL_KAFKA_INDEX，且环境为C3、新随机schema／Topic时才生效；默认路径保留。生产Queue、消费者和Topic配置入口未改 |
| 本报告 | 本轮结果与历史验收分开记录 |
| artifacts下的新驱动及索引 | 验证资源身份、单项执行、原始日志、失败夹具快照、证据汇总与字节重建。未覆盖旧脚本或报告 |

测试Topic使用segment.index.bytes=65536、file.delete.delay.ms=1000，以减少三分区索引预分配与测试结束后的临时文件等待。分区数、确认、幂等、消息大小、保留期、禁止非同步Leader及所有Offset验证保持原合同；没有对已有Topic执行配置变更。这些参数只是小数据故障夹具，不构成容量或性能结论。

完整清单见 [file-manifest.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/final/file-manifest.json)，基线与最终字节分别见 [基线归档](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/baseline-source.tar.gz)、[最终归档](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/final/final-source.tar.gz)。

## 2. 复用的回滚与恢复合同

[CODE] 回滚继续使用 PLANNED → COMMITTED → API_READY → RESUMED。Begin要求当前读目标为B、目的为原A、新冻结和预期修订号一致，并引用当前已提交推广。冻结闸门、选择器和操作行按既有顺序加锁；方向、目的、修订号和反向引用不可由重试改变。

回滚证明使用A的legacy两个消费组、投递状态、心跳和真实进程观察。它没有把B的owner lease当成A写入权。原始FetchOffsets必须包含每个分区的已提交位置，并严格越过本次精确标记，位于实际保留范围。完整MySQL／ES／CK对账、源边界与真实writer均重新核验。提交时再次核对这些事实和所有受控旧B API实际退出，在一个MySQL事务写选择、修订号、操作阶段与审计。

| 关键源码／symbol／准确行号 | 直接保证 |
| --- | --- |
| [gate.go](../internal/projection/gate.go)，AcquireBusinessWrite:37、Freeze:110 | 业务事务持有共享闸门锁至提交；冻结取得排他锁并排空，内部CDC与标记不被冻结 |
| [read_reverse.go](../internal/syncpipe/read_reverse.go)，beginReadSwitch:27 | 同一控制入口处理正式反向意图、行锁／CAS、rollback_of及显式恢复引用 |
| [read_switch_control.go](../internal/syncpipe/read_switch_control.go)，ValidateReadSwitchDestination:349、waitReadMarker:442、proveReadDestination:528 | 当前冻结的精确标记经CDC发布；实际消息、提交、writer与全量对账证明 |
| [read_projection.go](../internal/syncpipe/read_projection.go)，ResolveReadDestination:35、RawOffsetProgress:147、ObserveWriter:184 | A/B统一解析但使用各自状态；原始FetchOffsets缺失／越界明确拒绝，不采用Queue.Progress回退 |
| [read_legacy_process.go](../internal/syncpipe/read_legacy_process.go)，RegisterLegacyObservation:44、LegacyProcess:141 | A当前进程、可执行身份、Kafka单成员和心跳；不是候选owner或跨主机fencing |
| [read_api_drain.go](../internal/syncpipe/read_api_drain.go)，ConfigureReadDrain:59、checkAPIDrained:185 | 枚举同机受控API，确认实际退出；仅发SIGTERM或端口关闭不算完成 |
| [read_switch_commit.go](../internal/syncpipe/read_switch_commit.go)，CommitReadSelection:28；原子写入:166、169 | 证明实时重验，选择和阶段单事务；结果未知先读持久事实；重复操作不重复增加revision |
| [read_api_selection.go](../internal/syncpipe/read_api_selection.go)，ResolveReadSelection:19、CursorTarget:43 | ES、CK、freshness和status统一解析；游标包含read_revision，Contract指纹不改 |
| [read_api_ready.go](../internal/syncpipe/read_api_ready.go)，checkReadReadyMode:85、ObserveAPIReady:342、ResumeReadSwitch:396 | 当前boot／PID、真实HTTP、权限与对账；新boot重验；OPEN与RESUMED同事务 |
| [read_cancel_resume.go](../internal/syncpipe/read_cancel_resume.go)，ResumeCancelledReadSwitch:16 | 提交前失败只能显式取消；恢复前重验当前B健康 |
| [read_rollback.go](../internal/projection/read_rollback.go)，readSelectionChain:71 | 连续读修订号与已提交审计链，回滚不减revision，不擦除Handoff历史 |

[RUNTIME] COMMITTED后不能取消。已有显式反向恢复专项重新完成A1→B2→A3（未就绪）→B4；旧未完成操作和其审计保留，业务仅在新API验证后恢复。API_READY后杀死A进程时，原boot与未经观察的新boot都不能Resume。

## 3. 本轮真实主链和新鲜证明

环境一使用项目既有C3私有实例：MySQL8.4.5:33315、Kafka4.4.0:19108（Controller19109）、ES8.17.5:19209、CK:18132。MySQL UUID为1d137b7a-e10a-11f1-acf3-adab4f8ac84c，Kafka cluster为3ZmDMgg1T_qwWGFTKvGnfA。每个测试创建独立随机业务schema、Topic、索引、CK空间和组；不接续或重置保留夹具的Offset。

[RUNTIME] 最终主往返运行时间为2026-11-16 05:05–05:06 UTC。A1服务、B重建和接续验证后，先完成正向推广；B活跃期间通过真实HTTP创建、评论及转派，再重新冻结、证明A、排空B、原子切回A3。回滚前11工单／49规范历史完全一致；恢复后再评论与创建，A/B均为12工单／51规范历史，首次解决6、重开2、差异和冲突0。

本次回滚操作为rollback，引用forward；冻结2b03068ddbdef547934180436c2e562b／revision5，专属marker为05930d3f3bfa795f2305b3594018275c。证明摘要为15d876077b67ee07b8cbcb3b793f7dddac5a90649a0f693c963e2978a778f873。

| 分区 | 本次marker消息Offset | A-ES committed | A-CK committed | 两组实际保留范围 |
| --- | --- | --- | --- | --- |
| 0 | 43 | 45 | 45 | [0,45] |
| 1 | 34 | 36 | 36 | [0,36] |
| 2 | 28 | 30 | 30 | [0,30] |

原始消息、目标UUID、源checkpoint、legacy实际进程、组成员及完整报告见 [fresh-rollback-proof.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/fresh-rollback-proof.json)、[原始FetchOffsets](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/legacy-raw-fetch-offsets.json)、[位置向量](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/proof-vector.json)。四组从前到后的提交单调、pinned位置与共享CDC身份不变，见 [source-offset-preservation.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/source-offset-preservation.json)。故障夹具中刻意破坏的位置不用于正常主链证明。

真实HTTP验收要求Search当前字段／业务版本和MySQL权限页一致，Analytics与独立MySQL规范指标一致，两种状态均来自A的legacy组／namespace／revision3。Gold窗口仍为创建10、首次解决6、平均首次解决60600秒；新增业务在窗口之外，只改变当前积压。主回滚时积压6，恢复后两张新待处理使积压为7。没有用CK物理行数相等替代规范历史对账。

| 可直接核验的结果 | 证据 |
| --- | --- |
| 完整往返、回滚前后数据、游标409／400、B Handoff不变 | [primary-roundtrip.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/primary-roundtrip.json) |
| 双投影规范事实和独立指标 | [full-reconcile.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/full-reconcile.json) |
| 实际旧B退出范围 | [api-drain.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/api-drain.json) |
| 当前A的HTTP Search／Analytics／权限和boot | [api-ready.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/api-ready.json)（SIGKILL恢复专项的完整就绪证据） |
| OPEN与RESUMED实际持久记录 | [api-resumed.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/api-resumed.json) |

## 4. A暂时落后及跨冻结旧证明

[RUNTIME] 新测试先取消一个已有回滚证明并在B恢复业务，再阻断本次随机A索引的文档PUT。B真实创建、评论、转派成功，候选全量对账一致；A原始提交落后于log_end，内容对账存在差异。B在新的回滚冻结前仍能提供真实Search。

A落后时，新的Validate因待处理投递未排空而失败，未产生可用证明。使用此前取消的证明或错误修订号也失败，无证明Commit失败。MySQL仍为candidate_v1／revision2、FROZEN、PLANNED。解除故障后A真实追平，同一合法新操作生成专属marker、重新完成完整证明，随后排空B并回切A3。

最终测试的新旧marker分别为0b727055f8b8c95d6ccffe4f2260d97c、c216208724d5bec090e8bafb08257d89；冻结分别为f03168bd7590118caf2eb36fb8a3ad60、ebc403f16bd354ef7211749d7a020d68。Commit和Resume各重复三次，读revision仍为3、闸门revision不重复递增。旧IT团队详情403、新device团队200，A恢复后再HTTP评论／创建，双投影12／51及Gold一致。

见 [拒绝状态](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/temporary-lag-refused.json)、[追平与恢复状态](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/temporary-lag-recovered.json)。这里验证的是跨冻结旧证明不可复用，不宣称存在额外的时间TTL；证明有效性由冻结、源边界、目标、writer和实际位置决定。

## 5. 故障、回归与复现命令

[RUNTIME] 本轮完成11个不同C3主体测试，其中原legacy故障矩阵只重跑内容缺失与错误修订号两项子场景，不能称为重跑全部14项矩阵。12个实际控制器SIGKILL边界为PLANNED、marker、proof、选择提交、API_READY和Resume事务前后；原始持久状态及被杀进程身份见 [sigkill-state-timeline.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/sigkill-state-timeline.json)。完整映射见 [requirement-matrix.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/requirement-matrix.json)。

| 范围 | 本轮结果／口径 |
| --- | --- |
| C3正常往返、暂时落后、12个SIGKILL、当前boot、在途B请求、并发、CLI、显式取消／恢复、重放、元数据、两类拒绝 | 本轮真实通过；初始准备失败另行保留 |
| C2双端口新版／未知选择器旧二进制排空 | 本轮1项通过 |
| C1新冻结精确marker与Gold | 本轮1项通过 |
| M4-03A事务排空与并发创建 | 本轮1项通过 |
| M4-03B新组精确位置与Gold | 本轮1项通过 |
| 完整M1–M5相关回归 | 新启动的既有C2隔离实例；MySQL78＋syncpipe11＝89项通过，race构建 |
| Go race／go vet／Python unittest | 本轮通过／通过／3项通过 |
| 全部C1、C2、A、B历史专项与M4-02多Broker矩阵 | 保留历史结果；本轮没有完整重跑 |

C3最终专项采用普通构建，已有保留race进程占据大部分内存。首次新延迟测试使用race测试二进制、普通worker成功；完整M1–M5、非集成Go测试重新采用race。没有把普通C3运行说成全进程race重验。各次源码快照与最终执行源码是否匹配均在 [test-index.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/test-index.json) 明确记录。

本轮的驱动和逐项命令均随资产保存，可从项目根执行；它们会核对隔离身份并拒绝演示端口。不要替换为原演示DSN，也不要在受保护运行器尚存活时替换其可执行文件。

~~~bash
python3 -B artifacts/m4-03c3/revalidation/20261116T041645.985246Z/revalidate_run.py build
python3 -B artifacts/m4-03c3/revalidation/20261116T041645.985246Z/revalidate_run.py test \
  --run '^TestM403C3TemporaryLegacyLagRequiresNewProofAndIdempotentResume$'
python3 -B artifacts/m4-03c3/revalidation/20261116T041645.985246Z/revalidate_run.py test \
  --run '^TestM403C3RealForwardRollbackRevisionCursorAndBothTargets$'
python3 -B artifacts/m4-03c3/revalidation/20261116T041645.985246Z/revalidate_run.py test \
  --run '^TestM403C3RollbackSIGKILLDurableStagesAndBootRecovery$'
# secondary_regression.py需要已核验且正在运行的C2私有基础实例。
python3 -B artifacts/m4-03c3/revalidation/20261116T041645.985246Z/secondary_regression.py
~~~

原始命令、时间、退出码和资源采样位于每次execution目录。完整89项及静态结果在 [secondary运行](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/execution/20261116T051541.868002Z-secondary/revalidation-summary.json)。

[CODE][RUNTIME] 默认Go包范围的go vet通过。额外开启全部集成标签的vet返回1，指出既有session_integration_test.go第144行的三个LoginRequest未命名字段字面量；该文件保持基线字节，属于此次额外检查发现的既存诊断，没有混入默认vet通过结论。原始输出见 [additional-tagged-vet.log](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/additional-tagged-vet.log)。

## 6. 失败保留、环境保护及字节交付

[RUNTIME] 本轮四次失败运行全部保留：两次私有总RSS超限、一次新夹具准备时磁盘超限、一次M5-03测试通过后清理观察阶段磁盘超限。没有隐藏或将这些批次改为PASS。受保护C3长期运行状态会自然增加Kafka／MySQL／CK数据，期间多次超过既定8GiB磁盘准入；测试运行器停止新实验，原保留服务继续运行。

中止夹具的MySQL完整快照、ES／CK事实、Topic身份、组位置、实际进程退出及有限清理结果另存。只回收本轮新随机夹具，原演示、历史夹具和失败证据未清理。最终完整回归转到已有C2私有实例：33314／19106／19208／18131，源UUID25f71628-e0cb-11f1-93cb-268d52f3cc26。该实例此前已停止，本轮核验后启动，完成测试后只停止这次新启动的基础进程，数据和证据保留。

[RUNTIME] 首次打包发现C2启动脚本把固定路径的baseline-mysql.json覆盖为本次PID88055。当前运行输出已另存；从历史ownership与原SHA-256清单核对原PID30496，恢复后的1772字节完全匹配原摘要007e92a74e62190ef1b67d7c4f95c078d2a3054da4bfa9ff32a76e982edcd101。首次打包失败及其源码归档仍保留，最终包写到独立final目录；没有把覆盖事件隐藏为“从未发生”。之后全部11,241个受保护证据重新核验。

| 保护／交付项 | 原始证据 |
| --- | --- |
| 四次失败、对应日志和夹具快照索引 | [failed-attempt-index.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/failed-attempt-index.json) |
| 原演示数据及011–015未安装 | [原演示核验](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/original-preservation/preservation/1794806373978578000/demo.json) |
| 原演示与七个C3保留进程身份 | [process-preservation.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/process-preservation.json) |
| 保留A3实际HTTP、四组单调位置、A/B全量对账 | [preserved-live-observation.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/preserved-live-observation.json) |
| C2仅停止本次新启动的基础服务 | [停止观察](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/secondary-runtime/environment-closeout/20261116T052033.384730Z/stop.json) |
| 固定输出覆盖事件与原字节恢复 | [historical-evidence-restoration.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/historical-evidence-restoration.json) |
| 历史11,241个证据文件SHA-256保留 | [historical-preservation.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/final/historical-preservation.json) |
| 基线／最终清单与实际变更 | [baseline-source-manifest.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/baseline-source-manifest.json)、[final-source-manifest.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/final/final-source-manifest.json) |
| 完整Diff及基线展开后逐字节还原 | [full.diff](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/final/full.diff)、[diff-verification.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/final/diff-verification.json) |

## 7. 最终范围

[RUNTIME] B→A正式回滚真实成功，A获得本次新冻结的完整证明。提交前失败保持B与冻结，提交后按持久阶段恢复；API死亡必须重新观察。旧游标没有在A3复活，四组正常主链提交保持正确，两套投影在恢复业务后继续增量追平。本轮没有新增自动降级、自动回滚或Offset初始化路径。

[INFER] 这些结果满足当前要求的macOS单机受控往返验收，已有生产实现可以保留。若A丢失位置、越过保留期、目标身份变化、出现未解决隔离或无法证明进程身份，仍应按既有协议拒绝回滚，不能借助重置Offset恢复许可。

[UNKNOWN] 任意未登记进程、外部自动重启器、跨主机网络分区、请求级ES/CK fencing、Linux平台、十万规模冻结证明时间和生产高可用未在本轮验证。MySQL、CDC、ES、CK、API及单机磁盘／网络仍是单点。长期保留实例的资源增长已被实际观察，下一次实验必须重新核对预算，不能直接复用本轮成功时的资源截图。

没有暂存、提交、推送、部署。最终机器可核验结果见 [closeout.json](../artifacts/m4-03c3/revalidation/20261116T041645.985246Z/final/closeout.json)。
