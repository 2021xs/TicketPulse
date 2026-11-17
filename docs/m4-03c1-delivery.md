# TicketPulse M4-03C1：持久读目标选择与新鲜推广证明

C1 已实现持久读选择、PLANNED／CANCELLED 恢复合同，以及在新的业务冻结下重新验证目的投影的能力。真实验证使用新增的独立 MySQL、Kafka、ES、CK；API 仍按原启动配置读取旧 ACTIVE。本轮没有提交读选择、重启 API、迁移 Offset 或执行正式回滚。最终验收结果以 [test-summary.json](../artifacts/m4-03c1/test-summary.json) 和 [closeout.json](../artifacts/m4-03c1/closeout.json) 为准。

本报告区分四类证据：[CODE] 当前实现；[RUNTIME] 本轮真实执行；[INFER] 方案及边界判断；[UNKNOWN] 尚未验证。所有源码定位均重新从 live repository 读取。

| 术语 | 本轮含义 |
| --- | --- |
| 登记 ACTIVE／PREPARED | 保留 A/B 的登记及写入归属语义，不表示新的 API 读切换已经完成 |
| 读选择 | 单例记录，统一引用一个版本、合同摘要和状态来源；C1 初始化为原 ACTIVE |
| 读修订号 | read_revision；本轮固定为1，正式切换递增属于C2 |
| 新鲜证明 | 新冻结下的精确源标记、实际提交向量、当前写入者及完整内容对账 |
| CANCELLED | 未提交读选择的显式取消；保留操作与证据，不删除历史 |

## 1. 基线和实际改动

[CODE] 分支为 `feat/m1-01-ticket-loop`，没有有效 HEAD，Git index 为空，文件均未跟踪。本轮没有用 git diff 判断实际改动。实施前保存263个源码、配置和文档文件的大小、模式与SHA-256，归档原始字节；其中M4-03B最终清单的262个文件全部匹配，另一个文件为C0调查报告。关键源事实在修改前另行固定。

| 基线产物 | 用途 |
| --- | --- |
| [baseline-source-manifest.json](../artifacts/m4-03c1/baseline-source-manifest.json) | 全部实施前源码、配置和文档的哈希与模式 |
| [baseline-source.tar.gz](../artifacts/m4-03c1/baseline-source.tar.gz) | 没有HEAD时可重建的原字节 |
| [preflight-source.json](../artifacts/m4-03c1/preflight-source.json) | 与B最终清单比较，确认无既存代码差异 |
| [source-facts.json](../artifacts/m4-03c1/source-facts.json) | 冻结、标记、Offset、Reconcile与API调用链事实 |
| [previous-evidence-manifest.json](../artifacts/m4-03c1/previous-evidence-manifest.json) | 6,451个历史证据文件的实施前哈希 |

[CODE] 实际改动如下。没有修改原Version.Contract、Handoff、consumer group生成、pinned offsets、writer ownership、CDC发布协议或M3统计SQL。原API二进制入口、Search/Analytics读路径、标准消费者源码保持实施前字节。

| 文件 | 职责和影响范围 |
| --- | --- |
| [013_projection_read_control.sql](../db/migrations/013_projection_read_control.sql) | 两张控制表；没有业务DDL、默认候选或Offset写入 |
| [db/embed.go:41](../db/embed.go#L41) | 显式迁移资源；API普通启动不执行 |
| [projection/read_control.go:32](../internal/projection/read_control.go#L32) | 选择／操作模型、合法初始化、持久读回、摘要及恢复守卫 |
| [projection/gate.go:186](../internal/projection/gate.go#L186) | 在原Resume事务中添加C1守卫；业务FOR SHARE／Freeze排空机制保持 |
| [syncpipe/read_projection.go:33](../internal/syncpipe/read_projection.go#L33) | 同一版本解析ES、CK、两个组与状态namespace；复用A/B身份检查 |
| [syncpipe/read_switch_control.go:50](../internal/syncpipe/read_switch_control.go#L50) | 初始化、登记、取消、精确marker、真实位置及完整对账证明 |
| [ticketpulse-control/main.go:67](../cmd/ticketpulse-control/main.go#L67) | 操作者CLI入口；没有公开维护HTTP接口或切换提交命令 |
| [m403c1_read_control_integration_test.go](../internal/mysqlstore/m403c1_read_control_integration_test.go) | 新实例的控制、标记、身份、隔离、CLI和SIGKILL实验 |
| internal/mysqlstore/integration_test.go、m403a_control_integration_test.go、m403b_handoff_integration_test.go；internal/syncpipe/reconcile_integration_test.go | 仅增加C1私有socket／端口白名单，B网络故障注入改用所选私有端口；原断言保留 |
| [m403c1_environment.py](../scripts/m403c1_environment.py)、[m403c1_suite.py](../scripts/m403c1_suite.py) | 新进程、数据目录、端口、身份核验、资源预算及回归日志 |
| [m403c1_preservation.py](../scripts/m403c1_preservation.py)、[m403c1_package.py](../scripts/m403c1_package.py) | 原演示只读观察、源码与历史哈希、完整Diff和逐字节重建 |
| 本报告 | 实施合同、运行证据与C2边界 |

完整文件清单和原／新字节见 [file-manifest.json](../artifacts/m4-03c1/file-manifest.json)、[完整Diff](../artifacts/m4-03c1/full.diff)、[最终源码归档](../artifacts/m4-03c1/final-source.tar.gz)。历史文档，包括A/B与C0报告，保留原字节。

## 2. 013安装、功能初始化与读选择

[CODE] 013只创建 `sync_projection_read_selection` 和 `sync_projection_read_switches`。前者的selector_id主键及CHECK只允许1，引用一个version_id/hash、可空handoff、read_revision和当前操作ID。它没有独立的ES或CK选择列，因此控制入口不能分别提交两者。后者引用from/to版本与摘要、预期读修订号、确切freeze、阶段、操作revision、marker、proof及其摘要、错误和时间。marker唯一键防止不同操作重复占用同一个标记。

安装与启用是两个动作。[EnsureReadControlSchema:136](../internal/projection/read_control.go#L136) 要求011/012已显式安装，建表后只在原控制日志记录安装事实，不创建选择行。[InitializeReadSelection:50](../internal/syncpipe/read_switch_control.go#L50) 在业务冻结保护下执行原Managed检查：MySQL源、Topic／分区、ES索引、CK表身份、原组提交与进度binding必须匹配原ACTIVE。随后同一事务插入读选择和enable记录。初始化不接受PREPARED，不因表存在就默认选择某个候选。

[CODE] 合法初始化后，底层读回不仅查行的格式，还核对登记合同摘要和确切enable日志。本轮没有合法推进read_revision的接口，读取到不同版本、候选handoff、revision不为1、缺行或损坏时保守拒绝。阶段定义包含未来COMMITTED/API_READY/RESUMED，但C1不能进入这些阶段，也不能通过它们恢复业务。C2必须增加正式CAS和阶段合同后才允许读修订号递增。

| 环境状态 | 当前行为 |
| --- | --- |
| 没有013、从未启用 | 原A/B路径兼容，普通Resume按原条件执行 |
| 已安装013、未初始化，控制表为空 | 功能尚未启用；不会选取候选，原恢复路径仍兼容 |
| 已初始化且记录完整 | 选择原ACTIVE、revision=1；允许C1登记与新证明 |
| 有控制记录但无启用日志 | 元数据不完整，拒绝静默退回legacy |
| 明确启用后选择／操作表或关键行缺失 | 读控制与Resume拒绝；不重新初始化来掩盖丢失 |

[INFER] 这些保证适用于当前受控代码，不约束任意SQL客户端或仍运行的旧二进制。C1未把新选择接到HTTP读准入；这是C2的明确边界。

## 3. PLANNED与CANCELLED恢复合同

[CODE] [BeginReadSwitch:78](../internal/syncpipe/read_switch_control.go#L78) 依次取得连接级控制锁、冻结闸门FOR SHARE、选择单例FOR UPDATE和操作行锁。闸门锁阻止并发Resume，选择锁与CAS阻止并行意图。登记前核对确切freeze_id/revision、当前读选择和expected_read_revision、目的合同及身份、候选VERIFIED接续，要求两边属于同一个业务源和Topic。事务写入PLANNED、单例current_operation_id与控制日志；version_id和read_revision保持不变。

同一operation_id重试只接受相同方向、目的和预期revision，并要求持久冻结／选择仍匹配。不同操作或改意图被拒绝。控制锁是短期并行保护；CLI崩溃后真正保留维护边界的是MySQL中的PLANNED和选择引用，不能依赖进程仍持锁。

[CODE] [CancelPlannedReadSwitch:205](../internal/syncpipe/read_switch_control.go#L205) 只取消尚未提交的PLANNED。它再次核对原版本/hash/read_revision、确切freeze和单一未完成操作，在一个事务写CANCELLED、增加操作revision、清除选择引用、保存完整取消行到原控制日志。重复取消在同一冻结下读回相同终态；冻结已结束、修订号或选择变化后拒绝旧取消。取消本身不会自动OPEN闸门。

[CODE] [CheckReadResume:278](../internal/projection/read_control.go#L278) 在原Resume持有排他闸门锁时运行。它拒绝任何未完成读操作、坏摘要、丢失引用、冻结错配和C1不能完成的未来阶段。CANCELLED还必须与完整取消日志的规范JSON摘要相同；直接SQL修改阶段、清引用，或改写曾经取消的意图，都不能冒充合法取消。只有合法取消且原B恢复条件仍成立，普通Resume才可开启业务。

| 持久事实 | 恢复方式 | 是否可重试 |
| --- | --- | --- |
| PLANNED、marker为空 | 重新验证并创建marker；普通Resume拒绝 | 相同operation可重试 |
| PLANNED、marker已提交 | 继续同一marker，不读最新watermark推断ID | 身份／冻结匹配才可重试 |
| PLANNED、有proof | proof只是当次观察；再次Validate重新查实时事实并全量对账 | 不直接重用旧proof放行 |
| 验证失败 | 保留marker及last_error，清除操作当前proof；先处理原因，再同操作重做 | 不自动改Offset／抢writer |
| CANCELLED、选择未变 | 在同一冻结下显式Resume，原A/B守卫继续生效 | 取消读回可重试，Resume结果未知先查闸门 |
| COMMITTED/API_READY/RESUMED | C1恢复拒绝 | 属于C2/C3，不由本轮模拟完成 |

## 4. 新冻结下的新鲜证明

[CODE] [ValidateReadSwitchDestination:365](../internal/syncpipe/read_switch_control.go#L365) 最长五分钟，调用者可以设置更短期限。每次重新解析目的版本并验证源、Topic UUID／分区、ES索引UUID、CK表UUID、CK数据库UUID及当前组位置；缺提交、越界、身份不明或隔离记录立即拒绝。短暂processing/retrying记录可以在有限期限内排空，期间不能保存成功证明。

CK数据库UUID是C1额外保存的身份事实。[readCKDatabaseUUID:115](../internal/syncpipe/read_projection.go#L115) 从system.databases读取Atomic数据库的非零UUID，并将其固定在PLANNED和proof。原A/B Contract没有该字段，本轮没有重写它；仍复用其中的端点、空间名和两张表UUID。

[CODE] [ensureReadMarker:272](../internal/syncpipe/read_switch_control.go#L272) 在一个MySQL事务内更新源标记行，并将生成的精确marker_id/issued_at写入操作。事务未提交时两者一起回滚；提交结果未知时只读回操作。已持久marker不会因周期水位更新而重新生成。这个入口不直接向Kafka发送标记；现有CDC从已提交Binlog读取并按原协议发布到所有分区，源检查点仍只在整笔事务全部获得Kafka确认后前进。

标记提交后，验证事务再次持有冻结闸门、选择和操作行，阻止Resume或Cancel跨过正在执行的证明。等待及审计按以下顺序完成：

1. 对目的ES/CK分别使用原始FetchOffsets，并逐分区验证返回结果，缺位置明确失败。允许committed等于log_end，要求start≤committed≤end；候选还不能退到pinned之前。
2. 按operation的精确marker查各分区applied delivery，要求committed严格大于该消息Offset。
3. 用现有只读ReadRecord读取实际Kafka消息，核对marker、源UUID、issued_at和同一Binlog事务位置；两组所有分区都必须确认。
4. 核对候选持久owner、epoch、lock连接、组、单成员Stable、近期heartbeat和零未完成／隔离。legacy分支使用原组与原namespace，不建立新lease。
5. 调用既有全库Reconcile，MySQL前后摘要相同，ES/CK完整内容、规范事实差异和冲突均为0。q=nil只表示调用者已确认边界；本入口已经在前面独立确认真实标记，不以q=nil单独证明追平。
6. 审计后再次核对身份、当前writer／成员和原始位置；owner变化或位置倒退拒绝。保存完整报告及摘要、前后组观察、实际消息、冻结与版本关联到操作和控制日志。

[CODE] 新实现复用 [HandoffControl.checkIdentities](../internal/syncpipe/handoff_control.go#L75)、[rawOffsets](../internal/syncpipe/handoff_control.go#L291)、[Queue.ReadRecord](../internal/syncpipe/queue.go#L378)、[Reconcile](../internal/syncpipe/reconcile.go#L184) 及原标准消费者的delivery／heartbeat。没有新ETL、Kafka候选消费者、Handoff.Init、OffsetCommit或M4 follower接管。

[RUNTIME] 主实验先完成B VERIFIED，显式恢复业务，再通过真实HTTP给工单4追加评论到v11，然后第二次冻结并登记新操作。新证明对应10工单／47历史，首次解决6、重开2，source before/after相同、difference_count=0，ES/CK冲突0。旧B Verify再次调用仍只是历史身份观察，PLANNED不会因此自动获得proof。原十工单Gold由独立冻结预期核对。

| 主实验事实 | 值 |
| --- | --- |
| 新freeze_id / revision | 572c96160b16e8d6d4bc45204a75bb69 / 3 |
| read_revision / stage | 1 / PLANNED |
| 本次marker_id | 7bcfe14d4aaa35cd1ef8edafda5404f5 |
| CK数据库UUID | 32ea089e-ab0a-41dc-a5e9-7abb1cb8bf4b |
| ES和CK各分区marker Offset | 0→36，1→31，2→24 |
| ES和CK各分区实际committed | 0→37，1→32，2→25；全部保留范围内 |
| 完整报告SHA-256 | cd707539a13792f2e6f7d3d07a8034a9b4ae1c86548ee83aa702aba81e16fd05 |
| 完整proof SHA-256 | c0b28efae58d94ff89aef41fd218230b1d1454cabd2f02794d311f191116bb48 |

完整原始数据见 [primary-proof.json](../artifacts/m4-03c1/primary-proof.json)、[proof-vector.json](../artifacts/m4-03c1/proof-vector.json)、[full-reconcile.json](../artifacts/m4-03c1/full-reconcile.json) 及 [主实验日志](../artifacts/m4-03c1/runs/20261115T024912.213379Z-focused/integration.log)。摘要来自规范JSON；文件排版或MySQL JSON键顺序不是业务摘要。

## 5. 故障矩阵与恢复证据

[RUNTIME] C1专项12个顶层测试通过，包含5个独立身份故障子场景和5个真实SIGKILL边界。子进程辅助测试在父运行中SKIP，实际由崩溃测试创建子进程执行；不将SKIP计为PASS。用户18项要求及额外守卫映射见 [fault-matrix.json](../artifacts/m4-03c1/fault-matrix.json)。

| 故障／场景 | 实际观察 |
| --- | --- |
| 8并发Begin、重复ID、改方向 | 一项成功，其余明确冲突／锁拒绝；原read_revision不变 |
| PLANNED后Resume、取消后Resume | PLANNED拒绝；合法取消才允许按原B条件恢复，CANCELLED历史保留 |
| 修改freeze revision、删除操作／选择行 | Cancel、Status或Resume保守拒绝，不补写默认候选 |
| 手工改CANCELLED并清引用 | 无完整事务日志拒绝；修改已取消意图也不能使用旧取消日志 |
| VERIFIED后新业务、新冻结 | 历史报告不等于新许可；新marker和全量对账真实通过 |
| ES分区2标记进度写失败 | 另外两分区成功仍无proof；该标准worker退出。解除故障、重启后从当前commit接续同一marker并成功 |
| 真实删除候选一分区commit | 在明确停止的私有组内注入；C1拒绝缺位置，不重新初始化。该故障组不用于业务接续 |
| legacy新空组原始读回 | 缺位置明确READ_OFFSET_MISSING，无log_start回退；未执行逆向回滚 |
| ES、CK表UUID、CK数据库UUID、Topic UUID、分区集合变化 | 五个独立私有场景均拒绝；CK数据库故障保留两张表UUID，仅数据库UUID变化 |
| 真实非法Kafka消息、lag=0 | 候选隔离记录存在，三个分区lag为0仍拒绝，旧proof清除、marker保留 |
| marker事务前／后SIGKILL | 前者marker为空；后者确切marker已持久，无“只发布而操作不知ID”的成功合同 |
| proof保存前、事务提交前／后SIGKILL | 前两者无proof；提交后完整proof存在，PLANNED和Resume拒绝持续有效 |
| 相同操作重试 | 重新核对实时身份／组／位置并全量审计，已提交marker不更换 |
| 真实维护CLI | migrate-read-control、read-init、read-begin、read-validate、read-status、read-cancel成功；没有read-commit入口 |
| 未安装／已安装但未启用013 | 原业务与Resume路径正常 |

[RUNTIME] 初期失败日志保留，没有删除或改写：测试API的DeleteOffsets参数类型错误、NULL proof读回错误、processing状态的短暂竞争、故障worker实际退出后未重启、测试多语句配置错误均已定位并修正。隔离测试改为真实非法Kafka消息，避免被正在重放的成功消息覆盖。最终专项及所有回归以最终源码哈希重新执行；这些早期失败不作为通过证据。

五个SIGKILL边界的持久读回及子进程日志在最终focused运行的 `verification/TestM403C1MarkerAndProofSIGKILLRecovery/`。marker已提交后，各次恢复沿用同一个ID；证明未提交前不会出现成功摘要。

## 6. 相关回归、环境和复现

[RUNTIME] 独立实例：MySQL8.4.5/33313、Kafka4.4.0单Broker/19104与Controller19105、ES8.17.5/19207、CK/18130。全部有效进程、源UUID、数据目录、端口和集群身份先验证再创建随机业务库／Topic／索引／CK空间。只读发行包来自已固定的本地发行资产，业务资源不复用B随机库。

资源守卫限制私有RSS与磁盘各8GiB，最低空闲磁盘20GiB；CK使用4GiB tracker。每轮原始resource_samples保存实际观察。没有十万容量实验，没有新增内存来证明查询优化，也不宣称跨机器HA。

| 运行 | 命令 | 最终结果与日志 |
| --- | --- | --- |
| C1专项 | `python3 scripts/m403c1_suite.py focused` | 12顶层PASS、5身份子场景、5SIGKILL边界；[日志](../artifacts/m4-03c1/runs/20261115T024912.213379Z-focused/integration.log) |
| A基础 | `python3 scripts/m403c1_suite.py foundations` | 7项PASS；冻结排空、只读、CDC、稳定密钥游标、身份检查 |
| B交接 | `python3 scripts/m403c1_suite.py handoff` | 21项PASS；精确offset、重放、重平衡、UUID／保留范围、独立状态、CLI |
| M1–M5相关 | `python3 scripts/m403c1_suite.py regression` | MySQL78项+syncpipe11项PASS；业务权限、搜索、M3 Gold、重建及M5局部回归 |
| 静态 | `python3 scripts/m403c1_suite.py static` | Go race单测、go vet、3个Python测试PASS |

各轮精确命令、启动／结束时间、源码哈希、身份和资源采样链接统一列在 [test-summary.json](../artifacts/m4-03c1/test-summary.json)。没有重新运行历史M4-02三Broker矩阵或M5长时间容量实验，旧报告只作历史边界证据。

完整复现的独立入口：

```bash
python3 scripts/m403c1_environment.py start
python3 scripts/m403c1_suite.py focused
python3 scripts/m403c1_suite.py foundations
python3 scripts/m403c1_suite.py handoff
python3 scripts/m403c1_suite.py regression
python3 scripts/m403c1_suite.py static
python3 scripts/m403c1_preservation.py
python3 scripts/m403c1_environment.py stop
python3 scripts/m403c1_package.py
```

操作者协议由独立夹具真实执行如下；维护DSN和目标配置由已核验的私有环境传入，不把凭据写进报告：

```bash
ticketpulse-control -mode=migrate-read-control
ticketpulse-control -mode=read-init -version=active_original
ticketpulse-control -mode=read-begin -operation=operator_plan -version=candidate_v1 -read-revision=1
ticketpulse-control -mode=read-validate -operation=operator_plan -wait=30s
ticketpulse-control -mode=read-status -operation=operator_plan
ticketpulse-control -mode=read-cancel -operation=operator_plan
```

这些命令不是对原演示执行的操作，也不代表可以跳过身份、冻结和B VERIFIED前提。

## 7. 原环境与历史资产保留

[RUNTIME] 测试前后只读核对原演示：九张业务／身份表内容、ES全部文档字段与external version、CK全部物理行与既有基线一致；演示中011/012/013控制表仍未安装，原MySQL/Kafka/ES/CK/API端口持续可访问。C1启动／停止只向匹配C1目录和进程身份的PID发信号，私有数据目录保留。原演示没有冻结、迁移、停止、setup/reset或Offset操作。

原CDC持续运行会发布周期标记，因此其检查点和两个原组提交会自然前进。本轮保留前后原始观察，不要求它们数值静止，也不把自然推进误称迁移。原位置是否单调和进程仍在由收尾证据判断；业务、投影内容及目标身份保持不变。

[CODE] 包装阶段再次核对6,451个历史文件的SHA-256，以及关键A/B合同、标准消费、CDC和原API源码字节；结果见 [previous-evidence-preservation.json](../artifacts/m4-03c1/previous-evidence-preservation.json) 和closeout。完整Diff在临时目录从基线应用后比较全部最终字节，最终归档也逐文件读回，结果见 [diff-verification.json](../artifacts/m4-03c1/diff-verification.json)。没有暂存、提交、推送。

## 8. 边界、风险与C2准确缺口

[CODE] C1控制代码没有修改Kafka Offset。正常CDC和标准消费仍按原事务确认、投影成功、持久进度、Commit顺序工作。测试中的DeleteOffsets、Topic／UUID变更仅用于新随机私有资源；C1遇到这些故障拒绝验证，不负责修复。CK事实和ES external version保护保持原实现。

[INFER] 新proof是冻结下某一时点的完整观察，不能永久保证writer或后端健康。验证后owner、身份、保留范围和提交状态仍可能变化；操作状态明确把proof标为需要实时重验。C2提交必须在持久freeze和操作锁保护中再次检查，不能仅判断proof_sha256非空。当前MySQL行锁／owner观察不等同ES/CK请求级fencing，网络分区与任意非合作writer仍是边界。

[CODE] legacy分支已经解析原ACTIVE合同和两个原组，复用身份／完整对账与严格原始Offset读回；没有增加旧writer lease。其单成员和共享heartbeat只提供当前组观察，不能证明任意进程的后端写入互斥。[UNKNOWN] 新冻结下正式逆向回滚和旧投影生命周期在C3真实验收，本轮不声明已验证。

C2至少需要以下精确接口和调整：

| 后续接口／改动 | 要关闭的窗口 |
| --- | --- |
| CommitReadSelection(operation_id, expected_revision, proof_hash, api_drain_evidence) | 在同一事务更新version/hash/handoff/read_revision和COMMITTED；不能分别切ES/CK |
| ResolveReadSelection、CheckReadSelection | 从合法持久读选择同时构造ES/CK/freshness/status，替换当前C1只接受初始ACTIVE、revision=1的保守限制 |
| PLANNED读／启动准入与受控API排空记录 | 旧API读请求不被业务冻结阻止；旧二进制不认识新选择器，需证明退出和禁用自动重启 |
| ObserveAPIReady、ResumeReadSwitch | boot身份、选择修订号、两条真实API读路径验证与API_READY；OPEN+RESUMED+日志原子提交 |
| CheckFreshProofForCommit | 在新提交边界重新查精确marker、组／owner、UUID、保留范围和失败状态；历史proof不直接放行 |
| 读修订号游标绑定 | 每次正式切换增长，A→B→A不复活旧游标；同密钥验证409，真正过期保持400 |

[UNKNOWN] C1未验证API推广、API进程排空／重启、read_revision递增、A→B→A、真实正式回滚、十万级证明时长、多Broker故障期间C1验证、跨主机或请求级写入隔离、Linux/Docker。C0指出的共享CDC与旧ACTIVE检查依赖保持原样，没有扩大成协调服务或高可用改造。

## 9. 验收结论

1. [CODE][RUNTIME] 持久选择和PLANNED合同成立：合法初始化、并发CAS、SIGKILL持久性、完整取消和Resume拒绝已在真实隔离环境验证。
2. [RUNTIME] 新冻结下的新鲜证明真实跑通：B VERIFIED后新增业务，第二次freeze、新marker、全部分区实际消息及提交、当前writer和完整Gold／内容对账通过。
3. [CODE][INFER] C1没有新增Offset初始化或接管路径；缺位置／越界／隔离／身份变化拒绝。新的成本是持久控制记录和全量审计、有限期限下可能需要重试；瞬时健康观察及非合作writer仍不能作为永久许可。
4. [RUNTIME] 满足C1验收并具备进入C2实现的基础。这个结论不代表候选已成为API读目标；当前读选择仍原ACTIVE，revision=1。
5. [INFER] C2需补上上一节的提交CAS、统一读解析／准入、API排空与就绪、成功恢复事务及读修订号游标接口，并在新的完整独立环境真实验收。

只有最终专项、相关回归和字节重建核验全部通过，closeout才记录M4-03C1_PASS。
