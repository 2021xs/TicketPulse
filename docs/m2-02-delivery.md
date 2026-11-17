# TicketPulse M2-02：Kafka 迁移与可靠性验收

当前默认消息系统已切换为 Apache Kafka，原 Go Binlog 捕获、ES 外部版本保护、Search API 和 Web 继续工作。原有五张演示工单及其历史、评论、创建幂等记录保留；本机真实故障、HTTP 和 Chrome 验收通过。Kafka 的选用来自用户对分区、消费组、Offset 和后续规模实验的决策，不表示已证明 NATS 性能不足。

本报告记录 2026-11-08 的 live repository，分支为 feat/m1-01-ticket-loop。仓库仍无提交，未暂存、提交或推送。追加迁移任务到达前，M3 已有实现；本轮将其已有独立 CK 消费改接 Kafka 并回归，没有扩展分析范围。统计交付另见 [M3-01](m3-01-delivery.md)。

证据标记：[CODE] 为源码确认，[RUNTIME] 为真实执行，[INFER] 为设计判断，[UNKNOWN] 为未验证。以下结论以单机功能验收为限。

| 结论 | 证据 |
| --- | --- |
| 默认命令和 Go 依赖已使用 Kafka，NATS 不再运行 | [CODE] Makefile、go.mod；[RUNTIME] kafka-runtime.json |
| 整个源事务发布成功后才保存捕获检查点 | [CODE] Capture.Run；[RUNTIME] 部分发布中断测试 |
| 下游成功或持久隔离后才提交分区 Offset | [CODE] Queue.Consume；[RUNTIME] ES 成功后进度写入失败测试 |
| 失败分区不会被后续提交越过，其他分区可继续 | [RUNTIME] 三分区、两个组成员重平衡测试 |
| 迁移接续旧 Binlog 边界，没有清空业务库 | [RUNTIME] migration-result.json、business-preservation.json |
| M1、搜索和已有 CK 统计均通过真实回归 | [RUNTIME] verification-summary.json |

## 当前数据流与组件

已提交的 MySQL 事务由原 Go 捕获器读取。工单和同版本历史聚合成一条消息，按工单 ID 分区写入 Kafka。ES 与 CK 分别订阅完整 Topic，拥有各自的消费组和 Offset。业务 HTTP 只提交 MySQL，不直接发一次消息来代表可靠传播。

~~~mermaid
flowchart LR
    B[业务 HTTP / Service / Store] --> M[(MySQL 8.4.5)]
    M -->|ROW Binlog / 事务提交| C[原 Go CDC]
    C -->|ticket_id Key / acks=all| K[(Kafka 4.4.0 / 三分区)]
    K --> E[ES 独立消费组]
    K --> A[已有 CK 独立消费组]
    E --> ES[(Elasticsearch)]
    A --> CK[(ClickHouse)]
    ES --> S[Search API / 当前 MySQL 权限]
    CK --> D[Analytics API / 当前主管权限]
    S --> W[Web 搜索]
    D --> V[Web 看板]
~~~

这是结构示意图，实际版本和运行状态在 [Kafka 环境证据](../artifacts/m2-02/kafka-runtime.json)。

| 组件 | 实际版本或配置 | 本地入口 |
| --- | --- | --- |
| MySQL | 8.4.5；ROW/FULL/FULL；sync_binlog=1 | 127.0.0.1:33306 |
| Apache Kafka | 4.4.0；KRaft 单 broker/controller | broker 19092；controller 19093 |
| Java | Temurin 21.0.13+9，macOS arm64 | 项目忽略目录内独立安装 |
| Go Kafka 客户端 | franz-go v1.20.1；kadm v1.17.0 | module 最低 Go 1.24 |
| Elasticsearch | 8.17.5；原索引合同保留 | 127.0.0.1:19200 |
| 已有 ClickHouse | 26.10.2.103，独立 CK 消费组 | HTTP 18123；native 19001 |
| Web | 原内嵌页面、Cookie 身份 | http://127.0.0.1:18080 |

Kafka 使用副本数 1、最小 ISR 1，Topic 禁用自动时间与容量过期、禁用非同步副本选主。broker 设置逐条 flush 和一秒周期 flush；这些是本地运行参数，不构成多副本或断电恢复证明。发行包校验值保存在 artifacts/m2-02/kafka.sha512、java.sha256，安装脚本核对固定发行包。

## 消息、分区与两个进度边界

业务 Topic 为 ticketpulse.<database>.tickets.v1，默认三分区。工单消息 Key 是十进制 ticket_id，采用与 Java 兼容的 Key 分区方式；相同工单在固定分区数下进入同一分区。源水位标记是控制消息，显式写到每个分区。

| 消息字段 | 语义 |
| --- | --- |
| schema / kind / event_type | 协议 1/2 可读；新消息带动作类型、ticket_snapshot 或 watermark |
| ticket_id / version | 工单 ID 与业务版本；version 是 ticket_version 的实际字段名 |
| event_id / event_at | Binlog 消息由源 UUID、日志文件、事务结束位置、工单、版本确定；时间来自业务更新/历史 |
| source / commit | binlog 或 snapshot；实时事件包含源 UUID、文件与已提交位置 |
| document | 原 ES 当前投影字段；标题/描述全文，枚举与 ID 精确匹配 |
| history / first_resolved_at | 可选不可变业务历史及生命周期字段；供独立历史事实消费者使用 |

评论仍在独立业务表中，评论事务同时增加工单版本并写历史，因此触发该版本的工单消息。评论正文不加入全文索引。旧 schema 1 的当前快照只能恢复当前状态，不能凭当前团队推断历史统计；已有 M3 历史初始化补齐原 MySQL 历史事实。

捕获检查点表示“该源位置之前的相关已提交事务已经可靠发布”，消费 Offset 表示“这个分区之前的记录已经完成下游效果或持久隔离”。二者不能互相替代，更不能把多个分区的 Offset 相加当作连续水位。

| 进度/记录 | 保存位置 | 推进时机 |
| --- | --- | --- |
| CDC 检查点 | MySQL sync_kafka_cdc_checkpoint | 整个事务的输出与控制标记全部得到 Kafka 确认 |
| Topic 身份 | sync_kafka_pipeline | 记录 Topic ID、名称和分区数；重建或配置不符明确失败 |
| ES / CK Offset | Kafka 各自 Consumer Group | 下游写入与处理记录成功，或隔离记录持久成功后同步提交 |
| 成功/重试/隔离/恢复 | sync_kafka_es_deliveries / sync_kafka_ck_deliveries | 以 Topic、Partition、Offset 唯一记录，保留原因及隔离原文 |
| 源水位与心跳 | sync_source_watermark、sync_kafka_watermarks、sync_worker_state | 标记经同一 Binlog 和 Topic 流转；展示每分区确认情况 |

生产者开启 franz-go 默认幂等模式，使用 acks=all；每条同步发布，配置五次 record 重试、十秒交付期限，并限制缓冲与单条消息大小。进程重启后的幂等会话不能消除所有重复：若发布成功而检查点尚未保存，重读会再次发送。多条输出只发布一部分时也从原检查点重放整个事务，已发布部分允许重复，未发布部分不能遗漏。

消费者关闭自动 Offset 提交，最多一次取 16 条，分区间并行、每个分区内按顺序处理并提交下一 Offset。处理中阻止重平衡撤销归属，批次上限 45 秒，小于 60 秒重平衡期限。ES 每次最多三次有限重试；暂时错误将失败分区退回原 Offset、暂停并退避，其他分区继续。持续依赖故障可以继续重试，永久错误则先保存完整隔离记录再推进，避免永久坏消息无限占住分区。数据库控制记录失败会停止这轮消费并由进程监督重启，未确认记录仍可重读。

ES 文档 ID 仍等于工单 ID，严格 external version 只接受更高业务版本。重复和旧版本得到明确的 duplicate_or_old 处理结果，因此“ES 成功但 Offset 未提交”重放不会倒退或重复业务动作。消费不反向修改工单、评论、历史或创建意图。

## 安全切换与数据保护

切换前已保存 [完整源码基线](../artifacts/m2-02/pre-kafka-source.tar.gz)、[文件哈希](../artifacts/m2-02/baseline-source-manifest.json) 和 [业务状态](../artifacts/m2-02/pre-cutover-mysql.json)。基线包含 127 个源码文件，集合 SHA-256 为 5ba134405df5c08608d2ab38de21e71128a73bcd2490e062291590cc2b11d95d。M2-01 历史报告与证据未删除。

本机迁移先停止项目自己的旧捕获/Web，安装 Kafka 与 008 控制表，然后在原 CDC 命名锁下执行 migrate-kafka。新检查点使用仍被保留的旧 Binlog 边界，再导入当前工单快照；后续从该边界消费增量，切换期间写入包含在重叠窗口中。再次执行迁移保留已有 Kafka 起点。未迁移的旧检查点、失效日志、源 UUID 不符或同名 Topic 被重建都明确阻止启动，不静默跳到当前日志头。

[RUNTIME] 演示旧边界为 ticketpulse-bin.000001:23939814，旧检查点保持原值；当前 Kafka 检查点独立向前。隔离库测试在迁移前、快照期间和快照后分别通过真实 HTTP 写入，工单最新版本与完整历史均恢复。最终 [演示对账](../artifacts/m2-02/demo-reconciliation.json) 显示五张工单的 MySQL/ES/CK 投影一致，29 条历史事实一致，2 条评论和 5 条创建意图仍保留，两消费组积压为零。

迁移前后比较覆盖工单 ID/编号/版本/状态/当前团队/负责人及历史、评论、幂等数量。全业务表的最终哈希另有保存；迁移前没有采集所有字段的完整行哈希，因此不把数量和关键列比较表述为逐字节全表比较。

回滚时先停止当前捕获和消费者，使用保存的 pre-kafka-source.tar.gz 在独立目录恢复源码并构建，再启动保留数据的旧 NATS 及旧链路。旧 Binlog 仍须覆盖旧检查点；若已过期，应停止并重新评估初始化，禁止重置业务库或把旧检查点移动到日志头。回滚构建材料及旧运行数据已保留，完整反向回滚演练本轮未执行。

## 运行、恢复与扩展

在项目根目录按 [README](../README.md) 初始化本项目实例；已有 M1 数据不会重新导入 schema。

~~~bash
make setup
make kafka-setup
make build
# 仅已运行过 NATS 的库，停止自己旧 CDC 后执行一次
make kafka-migrate
# 两个终端
make sync
make run-search
~~~

已有库再次运行通常只需 make kafka-start，然后启动 sync 与 run-search。当前演示已运行在 http://127.0.0.1:18080/search。外部组件配置模板在 [sync.env.example](../configs/sync.env.example)；原生脚本把实际凭据放在忽略目录，报告和日志不输出它们。M2-only 可不设置 CK 两个环境变量；已有分析环境用 make sync-analytics / run-analytics。

~~~bash
make kafka-status
python3 scripts/local_kafka.py stop
make kafka-start
python3 scripts/local_kafka.py sync -mode=capture -server-id=1001
python3 scripts/local_kafka.py sync -mode=consume
# 修复隔离原因后，按状态记录中的分区与 Offset 恢复
python3 scripts/local_kafka.py sync -mode=replay -consumer=es -partition=0 -offset=123
~~~

恢复命令重发保留原文到同一工单 Key，并保存原位置和恢复位置；新位置完成后原隔离记录标为 recovered。示例位置必须替换为实际隔离记录。状态显示每个分区的日志起点、末尾、提交 Offset、积压及处理次数，保留失败和恢复原因。单看积压为零不表示全链路追平；源标记确认与投影对账提供更强证据。

[CODE] 分区数在创建 Topic 前由 TICKETPULSE_KAFKA_PARTITIONS 配置，范围 1–64。相同组可启动多个 consume 进程，Kafka 分配分区；有效并行度最多为分区数，捕获仍由同库单实例锁保护。[RUNTIME] 已测试同组两个真实稳定成员的重平衡与恢复。[INFER] 扩大现有 Topic 分区会改变 Key 路由，应采用停止/接续/重建或受控 Topic 迁移；当前代码检测分区数和 Topic ID 差异并拒绝隐式切换，没有实现在线重分区工具。

未来分析消费者使用另一个组即可独立读取相同日志，不与 ES 共享确认。本项目既有 CK 组已按此方式接入并回归；完整事实需要 schema 2 历史以及旧历史初始化，不能仅用当前快照替代。

## 真实验收结果与限制

以下命令均真实执行，未跳过失败用例或替换 Gold。日志与执行元数据集中在 [verification-summary.json](../artifacts/m2-02/verification-summary.json)。

| 验收 | 结果 | 证据 |
| --- | --- | --- |
| make test / vet / build；go test -race ./... | 全部退出 0 | final-code-verification.json 与对应日志 |
| make integration | MySQL 仓储 56 项通过；整套含单元合计 76 项通过，race 开启 | m1-integration.log |
| make integration-search | 7 项真实 MySQL/Binlog/Kafka/ES 测试通过，race 开启 | integration-search-result.json、integration-search.log |
| make integration-analytics | 2 项既有 CK 完整性/恢复回归通过，race 开启 | artifacts/m3-01/integration-analytics-result.json |
| 五项 M1 HTTP smoke | 全部退出 0 | http-regression-result.json |
| make smoke-search | 真正创建、生命周期、搜索、原生 Kafka/ES 故障通过 | smoke-search-result.json |
| M1 / Search / Analytics Chrome | 4 / 2 / 2 项通过；无跳过和 flaky | m1-browser-test-result.json、各 browser-test-result.json |

| 故障或竞争场景 | 实际观察 |
| --- | --- |
| Kafka 停止期间 MySQL 创建 | 创建成功；未发布事务结束位置领先于检查点；broker 恢复后补齐 |
| 发布成功、检查点保存前中断 | 第一条实际持久，检查点未越过原边界；重启重复第一条并补齐同事务第二条 |
| ES 停止 | 业务创建正常，Search 返回失败；保留未确认记录，恢复后投影补齐 |
| ES 成功、Offset 提交前中断 | 注入真实 MySQL 处理记录失败；Offset 未越过该记录，重启安全重复写 |
| 失败分区与两个组成员重平衡 | 该分区 committed=0/lag=20，其他分区已排空；恢复后无遗漏，已提交组重启读旧记录数为 0 |
| 重复/旧版本迟到、评论联动 | 最终文档版本不倒退，评论和历史不受消费重放影响 |
| 永久错误隔离后修复重投 | 持久原文、原因、原/新 Offset；原记录恢复完成可追踪 |
| 跨团队时 ES 仍旧归属 | 当前 MySQL 批量授权立即排除旧团队；无标题、总数或游标泄露 |

Search 保留关键词、工单号、状态/优先级/类别、团队/负责人、多选 AND/OR、时间区间、稳定排序与加密游标。候选只含 ID/版本/排序，返回字段来自当前 MySQL；版本不一致的候选也排除。准确总数仍明确 unavailable，扫描预算耗尽或 ES 超时/分片失败返回错误，不宣称部分结果完整。

一次真实 HTTP 跨团队协作工单从创建到关闭走完 12 个版本：创建→认领→处理→IT 评论→跨团队→新团队认领→处理→设备评论→首次解决→重开→再次解决→关闭。保留两条独立评论和 12 条历史，首次解决时间未被第二次覆盖，详细原文在 [collaboration-demo.json](../artifacts/m2-02/collaboration-demo.json)。这张工单来自隔离验收库，未混入演示数据。

[RUNTIME] 小样本顺序测量为 8 次创建和 20 次查询：创建至可搜索中位 1049.45 ms、最大 1083.91 ms；查询中位 15.61 ms、最大 22.30 ms，ES refresh_interval=1s。记录见 [timing.json](../artifacts/m2-02/timing.json)。这只是本机同步与查询时延观察，不是吞吐或 NATS/Kafka 性能对照。

[UNKNOWN] 多 broker 副本、节点故障、网络分区、在线分区扩容、大规模吞吐、磁盘满和断电恢复未执行。Kafka/CK 控制与物理重复记录尚无清理策略；目前无限保留会增长磁盘，后续需按 M4/M5 冻结保留和容量实验。多个消费者的单行心跳尚不代表逐实例监控。完整反向回滚和跨平台脚本也未验证。本机 M2-02 可靠替换目标已达到，生产级高可用和性能目标仍开放。

## 源码、迁移与差异索引

| 位置 | 关键职责 |
| --- | --- |
| internal/syncpipe/capture.go，Capture.Run | 原 Binlog 提交聚合、逐条发布、整事务检查点 |
| internal/syncpipe/queue.go，NewQueue / Ensure / Consume | producer 配置、Topic、分区并行与有序手动提交 |
| internal/syncpipe/consumer.go，Consumer.Run | ES 版本效果、有限重试、永久隔离 |
| internal/syncpipe/store.go，Delivery / ReplayPayload / FinishRecoveries | 持久处理与恢复审计 |
| internal/syncpipe/kafka_identity.go，VerifyQueue | 同名 Topic 重建保护 |
| cmd/ticketpulse-sync/migrate.go，migrateKafka | 旧检查点接续与快照重叠 |
| internal/mysqlstore/kafka_integration_test.go | 三项 Kafka 专属真实边界测试 |
| scripts/local_kafka.py；configs/sync.env.example | 本地 KRaft 与运行参数 |
| db/migrations/008_kafka_sync.sql | 006/007 后可重复新增 Kafka 控制表，旧业务/控制表不删改 |

主实现相对 pre-kafka 基线新增 995 行、删除 471 行，测试与夹具新增 455 行、删除 110 行。完整文件、哈希与分类口径见 [changes-manifest.json](../artifacts/m2-02/changes-manifest.json)，可审阅的 [changes.patch](../artifacts/m2-02/changes.patch) 由源码归档比较生成，不将无 HEAD 的整个工作区误算为本轮新增。

同步消费继续使用显式错误分类和有限 Retry；Kafka 协议、分区、Offset 与迁移边界见本项目的源码和真实验收记录。
