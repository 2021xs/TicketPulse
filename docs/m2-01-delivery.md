# TicketPulse M2-01：可靠捕获与实时搜索

本轮已把 MySQL 的已提交工单变更接到持久消息流、Elasticsearch 和 Web 搜索。M1 的业务写入、权限、历史和幂等仍由原 Service/Store 执行；搜索是异步投影，详情和访问授权继续来自 MySQL。开发基于分支 feat/m1-01-ticket-loop，没有暂存、提交或推送。

M1 开发前基线已完整归档为 [75 文件源码压缩包](../artifacts/m2-01/m1-complete-source.tar.gz)，逐文件哈希与 Git 状态在 [基线清单](../artifacts/m2-01/baseline-source-manifest.json)。聚合 SHA-256 为 `5d41298d43ddce99eebb628d179d039f0ab9485253c8549efce8d01ab885c6e8`。旧业务 Service、状态规则、权限动作实现及原有测试未修改。

主要结果与边界如下。CODE 表示源码核对，RUNTIME 表示真实运行确认，INFER 表示设计判断，UNKNOWN 表示本轮未验证。

| 结论 | 证据类型与位置 |
| --- | --- |
| 捕获的是 MySQL Binlog，不依赖业务 HTTP 回调 | CODE：capture.go；RUNTIME：直接 SQL 修改、未提交/回滚事务、真实业务 HTTP 验收 |
| 消息持久化确认后才保存捕获进度，ES 写成功后才确认消费 | CODE：capture.go、consumer.go；RUNTIME：MQ 停机、进度记录失败和恢复测试 |
| 相同或旧版本不会改写已应用的新版本 | CODE：严格 external version；RUNTIME：真实 MQ 重复/迟到、评论版本和快照重叠 |
| 旧团队不能借滞后的 ES 读标题、列表字段或未经授权总数 | CODE：只请求 ID/版本并批量复核；RUNTIME：HTTP 和 Chrome 暂停同步后转派 |
| 旧工单已首次导入，初始化可重跑 | RUNTIME：原演示库 5 个文档；隔离库的快照/增量重叠与重复初始化 |
| 后续 CK 可采用独立消费进度读取同一流 | INFER：流保留历史快照和事务位置；M3 统计、历史回填合同仍需设计 |
| 规模、高可用、断电恢复及完整回灌平台未验证 | UNKNOWN：属于 M4/M5；本轮提供本机小样本时延，不提供容量承诺 |

## 组件与取舍

本机实际运行 MySQL Community Server 8.4.5、NATS Server 2.15.2 的 JetStream、Elasticsearch 8.17.5，以及 Go 1.27.1 编译的两个进程。使用公开 Go Binlog 客户端 go-mysql v1.13.0 和 NATS 客户端 v1.47.0，保留项目 Go 1.24 最低版本。Elasticsearch 通过 net/http 接入，发行包自带 JDK。未引入 Kafka/RocketMQ、Redis、CK、Docker 或内部运行服务。

选择 JetStream 是为了在当前没有容器与 JVM 的本机提供真实持久 MQ 边界。它是磁盘流和独立持久消费者，能够确认发布、重投和保存消费进度；NATS 原生包与 ES 内置 JDK 均已校验官方哈希。MySQL 原启动参数关闭 Binlog，本轮在核验本项目实例后重启，数据目录和 UUID 不变。ROW/FULL、完整列名元数据、同步 Binlog 与事务日志落盘均启用。

| 配置 | 本轮值及含义 |
| --- | --- |
| MySQL | 127.0.0.1:33306；server_id=1；ROW/FULL；binlog_row_metadata=FULL；sync_binlog=1；日志保留 7 天 |
| NATS | 127.0.0.1:14222；监控 127.0.0.1:18222；磁盘目录 .local/search/nats-data；sync_interval=always |
| 每数据库消息流 | TP_<database>；主题 ticketpulse.<database>.ticket；Limits/File；64 MiB；拒绝新消息而不淘汰旧消息；无自动时间过期 |
| ES 消费者 | es-v1；显式确认；默认 AckWait=30s；无限重投；单次一条未确认消息 |
| Elasticsearch | 127.0.0.1:19200；单节点、1 分片、0 副本；512 MiB 堆；索引刷新 1s；仅回环本地开发配置 |
| 本机索引 | ticketpulse_local_tickets_v1；文档 _id=十进制工单 ID |

消息流达到预算时发布失败，捕获进度停止推进，MySQL 业务仍可提交。值守人员需根据状态增加容量或在后续保留策略中明确归档方案，不能静默删除尚需恢复的消息。原生运行设置面向本机演示；跨机器部署的认证、TLS、副本与资源策略不在本轮验证范围。

## 数据流与提交边界

业务链是浏览器/HTTP → 原 Ticket Service → MySQL Store → 一个工单、历史、评论事务。该事务提交后，Binlog 客户端解析行变更，在 XID/COMMIT 处按工单与版本汇总为消息。评论的关联工单更新会产生一个新版本；评论表文本不进入索引，关联历史仍随该工单事件保留。

完整异步链是 MySQL ROW Binlog → Capture → JetStream 同步发布确认 → MySQL 捕获检查点 → ES 持久 pull 消费者 → 严格版本写入 → MySQL 处理记录 → JetStream AckSync。捕获检查点只在整笔事务的相关消息均确认持久化后更新。没有相关工单的事务只用于周期推进安全位置，控制表写入不会造成每条检查点反复自发布。

| 消息字段 | 合同 |
| --- | --- |
| schema | 1；未知/损坏结构隔离 |
| event_id | Binlog 使用 UUID/文件/提交位置/工单/版本；快照使用导入批次/工单/版本 |
| ticket_id / version | 与完整工单投影相同；版本正整数 |
| event_at | 业务 updated_at，UTC 微秒；不是发送时间 |
| commit | Binlog 的源 UUID、文件和事务提交位置；快照无该字段 |
| document | 工单权威行，含初始/当前团队、负责人、时间和版本 |
| history | 同事务工单版本对应的前后业务快照与操作归属；ES 不解释它 |

源 UUID、ROW/FULL 配置和检查点文件会在启动时检查。检查点 Binlog 已过期则明确停止并报 CDC_BINLOG_EXPIRED，不会跳到最新位置忽略缺失变更。物理删除不属于 M1 合同，捕获会停止并要求处理；M4 才设计删除、投影重建与更完整的恢复控制。

## 首次导入、重复与恢复

首次启动先持久保存当前 Binlog 位置，随后在 MySQL 一致性快照中按 ID 导入旧工单。快照成功后标记完成，实时捕获从保存的位置开始，因此快照期间提交的新变更最终会被重放。中断的快照可以重做，导入批次标识不同但 ES 文档 ID 固定。单独 snapshot 命令不改捕获进度，也能在索引重建后重新发送当前行。

ES 使用 version_type=external，与业务 version 完全一致。新版本更新；相同或较旧版本返回版本冲突且记录为 duplicate_or_old，不写回旧内容。JetStream 的短期消息去重只减少重复发送；最终正确性由工单版本保证，不依赖去重窗口或物理 exactly-once。

暂时性 ES 连接、429 或服务错误先执行有限重试，再 NAK 延迟重投，不确认未写入消息。永久格式/映射/写入拒绝保存原 payload、工单、版本、错误和消息序号到 MySQL，之后才确认这一隔离处置。隔离不是投影成功。修复环境后可按序号 replay；成功重投后原记录标 recovered，原始错误与 payload 仍保留。损坏消息重投仍会再次隔离；本轮不提供随意编辑消息内容的入口。

捕获进度保存在 sync_cdc_checkpoint。ES 实际确认进度保存在 JetStream es-v1 的 AckFloor；每条处理尝试、成功、重试、隔离及恢复记录保存在 sync_deliveries。sync_worker_state 保存心跳、最近事件与错误。状态命令同时返回源位置、捕获位置、流序号、消费进度、积压和最近写入耗时。队列为空不能证明捕获已跟上源，API 的 caught_up 为 null，页面明确尚未确认全链路追平。

## 搜索、分页和当前权限

标题与描述使用 standard 分词；工单号、状态、类别和优先级为 keyword，人员/团队/版本为 long，时间为 date_nanos，优先级另外保存排序 rank。时间按 UTC 精确到微秒；搜索输入限定在 date_nanos 支持范围。中文使用标准分词行为，本轮未安装专用中文分析器。

接口支持关键词、工单号精确值、状态/优先级/类别/创建人/负责人/当前团队，以及创建/更新时间左闭右开范围。同字段多值 OR，不同字段 AND；关键词多个分词 token 用 AND，搜索标题或描述。默认 updated_at DESC，再按 ID DESC；指定 created_at、priority 或 id 也有稳定唯一顺序。

访问范围在每次请求的 MySQL 一致性只读快照中建立。ES 查询先约束组织与创建人/当前有效团队，随后只请求 ID、版本和排序值。MySQL 再批量按当前归属、账号、成员及团队有效性复核，并提供返回列表字段。索引滞后时无权候选被过滤；版本不匹配的有权候选也暂不展示，以免旧过滤/排序语义被当成当前结果。A→B 转派提交后的新请求立即遵守 B 团队归属；已开始请求沿用其开始时的快照，与 M1 详情相同。

分页扫描直到找到 page_size+1 个合法、版本匹配结果。返回的 has_more 仅代表确实存在下一条有权结果；游标使用 AES-GCM 加密、绑定用户/组织/查询和一小时有效期，位置只来自已返回工单。每批最多 100 个候选，每页最多扫描 2,000 个；预算不够明确返回 SEARCH_TOO_BROAD，不返回半页“完整”结果。总数始终 null/unavailable，ES 的原始总数从不返回。固定数据集分页已独立核对，持续更新期间不承诺跨页快照。

## 运行与复现

在项目根目录执行下列命令。运行脚本验证本项目实例，凭据只存忽略目录的 0600 文件，不需显示或复制到终端。

~~~bash
make setup
make search-setup
make build
~~~

两个终端分别运行：

~~~bash
make sync
make run-search
~~~

访问 http://127.0.0.1:18080/search，用原 Alice、工程师或主管账号登录。make run 保留只依赖 MySQL 的 M1 入口；开启搜索使用 run-search。当前会话与原工单数据不会因服务重启而删除。若原 Web 已占用端口，先停止自己启动的那个项目进程，再启动新版本。

~~~bash
make search-status
python3 scripts/local_search.py sync -mode=snapshot
python3 scripts/local_search.py sync -mode=replay -sequence=<隔离序号>
python3 scripts/local_search.py stop --component=es
make search-start
~~~

新迁移 006_search_sync.sql 只创建三张控制表，要求 M1 的 001–005 已完成，可重复执行；不重建业务表。新库仍加载 schema.sql、seed.sql 和 login_seed.sql，然后运行 search-setup 应用 006。专用 CDC 账号有复制读取权限；本机同步状态账号只能读工单/历史和 Binlog 状态、写三张控制表。API 仍使用原业务账号，未增加写链路依赖。

手动环境需提供以下变量。脚本在本机自动设置；不要将密码提交到仓库。

| 变量 | 用途 |
| --- | --- |
| TICKETPULSE_DSN | 原 API/业务数据库连接 |
| TICKETPULSE_SYNC_DSN | 相同库的工单读取、复制状态和同步控制表权限 |
| TICKETPULSE_CDC_ADDR / USER / PASSWORD | Binlog 复制连接；每个捕获进程使用唯一 server-id |
| TICKETPULSE_NATS_URL | 持久 MQ 地址 |
| TICKETPULSE_ES_URL / SEARCH_INDEX | ES 地址和数据库专属索引 |
| TICKETPULSE_CURSOR_KEY（可选） | 64 位十六进制密钥；未设置时服务每次启动随机生成，重启后旧游标需重新搜索 |

## 验收与源码位置

验收命令、退出码、时间和原始日志见 artifacts/m2-01 的 JSON 与日志。M2 专项创建随机 MySQL 库和独立流/索引，结束仅清理自己创建的资源。原 5 张演示工单、29 条历史与 2 条评论保留。必要截图是搜索筛选结果、旧团队无结果、ES 服务异常和基本响应式页面。

2026-11-08 的 [最终验证记录](../artifacts/m2-01/final-verification.json) 中 13 个 make 目标全部退出 0。完整结果如下。

| 验证 | RUNTIME 结果 |
| --- | --- |
| test / integration / vet / build | 全部通过；integration 为真实 MySQL 8.4.5 + race，72 个顶层 Go 测试通过 |
| 五组原 HTTP smoke | 创建/幂等、动作、生命周期、协作、会话全部通过 |
| 原 M1 Chrome | 4 个场景通过，18.7s；普通工单、转派、权限/恢复及分页交互 |
| M2 集成 + race | 4 组通过，14.242s；真实 CDC/MQ/ES、固定样本、快照重叠、重试及进度失败 |
| 原生进程故障 smoke | ES 停止/恢复、MQ 停止/恢复、整个同步进程重启均通过；未发布事务前检查点不推进 |
| M2 Chrome | 2 个场景通过，21.7s；真实登录/创建/搜索、筛选/分页/详情、旧权限、加载/错误及 ES 真停机恢复 |

实时小样本性能见 [时延原始数据](../artifacts/m2-01/timing.json)：8 次顺序创建到 Search API 可见中位 1,027ms，范围 931–1,041ms；20 次顺序查询中位 15.7ms，最大 21.3ms。样本仅包含本机小数据、常规 1 秒索引刷新；不表示吞吐、压力结果或生产 SLA。状态里的历史快照 event_at→应用时间包含旧数据的年龄，不作为实时同步时延样本。

[运行核对](../artifacts/m2-01/final-runtime.json) 确认原 5 个 ES 文档的版本、当前团队和状态与 MySQL 一致；两个验收夹具已清理。当前演示 Web 18080、同步进程、MySQL、NATS 与 ES 均运行。源码完整差异在 [changes.patch](../artifacts/m2-01/changes.patch)，逐文件前后哈希和行数在 [变更清单](../artifacts/m2-01/changes-manifest.json)：46 个新增/修改文件，无删除；实现 +2,683/-4 行，验收/测试 +1,437 行，按物理 Diff 行统计。文档、配置及依赖行数单独记在清单，不与实现混算。

~~~bash
make test
make integration
make vet
make build
make smoke
make smoke-actions
make smoke-lifecycle
make smoke-collaboration
make smoke-session
make browser
make integration-search
make smoke-search
make browser-search
~~~

| 源码位置 | 实际职责 |
| --- | --- |
| cmd/ticketpulse-sync/main.go | 捕获/消费进程、snapshot/status/replay 运维入口 |
| internal/syncpipe/capture.go、snapshot.go | 真实 Binlog 提交捕获、快照重叠与检查点 |
| internal/syncpipe/queue.go、consumer.go、store.go | JetStream、ES 消费、持久处理/隔离/恢复记录 |
| internal/search/elasticsearch.go、service.go | mapping/DSL、外部版本、授权分页与加密游标 |
| internal/mysqlstore/search.go | 当前用户/团队快照、批量授权、候选与新鲜度 |
| internal/httpapi/search.go、cmd/ticketpulse/main.go | 新 API、身份解析复用与可选搜索装配 |
| web/app.js、app.css、embed.go | 搜索页面入口、条件/分页/错误与详情导航 |
| db/migrations/006_search_sync.sql | 三张新控制表 |
| scripts/local_search.py | 已校验组件、MySQL Binlog 与本机启动 |

真实跨团队演示见 collaboration-demo.json：v1 创建、v2 认领、v3 开始、v4 IT 评论、v5 跨团队重新入队、v6 新团队认领、v7 开始、v8 设备评论、v9 首次解决、v10 重开、v11 再次解决、v12 关闭。最终负责人为设备 Agent，初始团队仍为 IT，12 条历史与 2 条评论，首次解决时间保留。

## 同步实现及后续边界

internal/syncpipe 的消费者调用显式错误分类和有限重试，按错误类别决定重试或隔离。该轮的 Binlog 捕获、持久消息流及 ES 投影均在隔离环境验证；结论仅覆盖本项目实际运行链路。

M2-01 的完整链和最低可靠性合同已具备。余下的规模基准、全文分词质量、索引生命周期、自动对账、旧日志过期后历史恢复、多人/跨机器部署和独立 CK 首次历史导入尚未验证。当前只支持 M1 的递增版本与软停用对象，直接 SQL 改业务字段时必须同时递增版本；测试中的直接 SQL 用于独立证明 Binlog 捕获，不是产品写接口。M3 可继续复用已保留的业务历史与消息流，不能用只保存最新工单状态替代历史事件统计。
