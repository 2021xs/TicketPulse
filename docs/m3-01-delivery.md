# TicketPulse M3-01：实时运营分析验收

TicketPulse 已提供三个核心运营指标、日期/团队筛选和主管 Web 看板。历史事实与当前状态分别进入 ClickHouse，十工单独立人工预期、真实故障恢复和 Chrome 验收通过。追加的 Kafka 迁移已替换原消息边界，最终复现使用 MySQL Binlog → 原 Go CDC → Kafka → 独立 CK 消费组。

本报告基于 2026-11-08 的实际源码和运行记录，分支 feat/m1-01-ticket-loop，未暂存、提交或推送。当前 docs 未发现完整 PRD 文件，业务口径采用用户提供的 PRD v0.2 §10.2 及其明确数值；固定输入与人工 Gold 分别保存为 tests/analytics/prd_ten_tickets.sql、prd_gold.json，没有调用正式 SQL 生成预期。

证据标记：[CODE] 源码确认；[RUNTIME] 真实测试；[INFER] 合理判断；[UNKNOWN] 未验证。

| 判断 | 支撑 |
| --- | --- |
| 历史团队直接来自对应版本历史，转派不重写既往归属 | [CODE] 历史初始化/投影；[RUNTIME] T01、T04 对照 |
| 重复与旧版本不会改变指标，查询不依赖后台合并 | [CODE] 业务键归并/argMax；[RUNTIME] 暂停 merge、重放与乱序 |
| CK 两表部分成功和写入后确认中断均可恢复 | [RUNTIME] 实际表故障和 MySQL 确认记录故障 |
| ES 与 CK 各有独立组，任一暂停不阻止另一方追赶 | [RUNTIME] 独立停止、Offset 比较与重启 |
| 十工单结果、团队归属、边界和四张积压明细匹配人工预期 | [RUNTIME] Go 集成、HTTP、Chrome |
| 页面刷新和确认水位分别展示，普通用户被服务端拒绝 | [CODE/RUNTIME] API、角色失效与浏览器测试 |

## 输入链路与两张分析表

ClickHouse 实际版本为 26.10.2.103 official build，HTTP 18123、native 19001。项目通过 Go 标准库 HTTP 协议写入，无新 CK 驱动依赖；解析完整响应并分类超时/永久错误，配置同步 INSERT、未知字段拒绝、查询限时。Kafka 的部署、版本和切换细节见 [M2-02](m2-02-delivery.md)。

原 M2 消息已有工单与历史结构，实时捕获将同事务工单/版本与历史匹配。M3 最小扩展协议增加可识别的 ticket/watermark、首次解决时间及历史初始化；Kafka 迁移继续兼容 schema 1/2。没有第二套 Binlog 捕获或 MQ。ES 消费只关心当前 document；CK 消费把 history 转成事实，把工单转成当前候选快照。

~~~text
业务 HTTP → Service → Store → MySQL 工单/历史同事务
             ↓ 已提交 ROW Binlog
          原 Go CDC → Kafka 同一 Topic
                         ├─ ES 组 → 搜索投影 / Search API
                         └─ CK 组 → 历史事实 + 当前候选快照
                                    → Analytics SQL → 主管 API → Web 看板
~~~

| ClickHouse 表 | 内容及身份 | 选择理由 |
| --- | --- | --- |
| ticket_events | 组织/工单/动作版本、稳定业务事件 ID、传输 ID、动作、发生时间、事件团队、前团队/状态、后状态、创建/首次解决时间 | 保存不可变业务事实，普通 MergeTree 追加；查询按业务键去重 |
| ticket_states | 工单/版本、complete、当前团队/负责人/状态、初始团队及时间 | 保存候选当前快照，普通 MergeTree 追加；查询按最大版本选择整行 tuple |

结构唯一来源是 [db/clickhouse.sql](../db/clickhouse.sql)。创建、首次解决、重开、转派、关闭、评论等事实来自原版本历史；转派同时保留原团队与新团队，事件团队采用该动作历史归属。初始化 JOIN 当前工单仅取不可变组织、编号和创建时间，不能以当前团队推断历史团队。

两张表都采用追加写入。传输可能重复，因此查询先按组织/工单/动作版本合并事实，再按最早动作版本取得创建和首次解决；当前状态按 (version, complete) 取最大 tuple。complete 使同版本完整 schema 2 行优先于旧 schema 1 不完整行。聚合 tuple 保留空负责人和空首次解决时间，不依赖单列 argMax 对 NULL 的跳过行为。

[RUNTIME] 固定样本重导入、重复创建/解决、旧版本迟到时，物理历史达 95 行，规范业务历史仍为 46 条；暂停两表 merge 后数值不变。普通 MergeTree 的合并不负责业务去重，ReplacingMergeTree 也未被用来宣称天然幂等。相同业务键的内容不可变来自 M1 历史合同；恶意构造相同键的矛盾事实不是本轮验证范围。

## 三组指标与时间合同

时间以 UTC 微秒存储，日期用 Asia/Shanghai 划分。API 窗口是北京时间日期转换后的左闭右开 [start,end)。创建和首次解决应用窗口与历史团队过滤，当前积压只应用当前团队，不接受“过去日期的当前积压”解释。

| 指标 | 日期依据 | 团队依据 | 计数/平均规则 |
| --- | --- | --- | --- |
| 每日新增 | 创建事件时间 | 创建时初始团队 | 每张工单一次 |
| 每日首次解决 | 第一次 resolved 的事件时间 | 当次历史负责团队 | 每单一次；平均为总(首次解决−创建)/首次解决数 |
| 当前待处理积压 | 当前观测状态，不受历史日期过滤 | 最新 current_team | 只计 pending/in_progress；待确认和关闭均排除 |

正式 SQL 位于 [internal/analytics/clickhouse.go](../internal/analytics/clickhouse.go)，先确定首次事件再过滤日期/团队，避免在某个团队或日期内把第二次解决误当首次。以下展示关键计算形态，完整参数化 SQL 以该源码为准。

~~~sql
-- 事实先按 organization_id,ticket_id,version GROUP BY。
-- 一张工单的创建与首次解决：
argMin(tuple(occurred_at, initial_team_id), version) -- action='created'
argMin(tuple(occurred_at, event_team_id), version)  -- action='resolved'
-- 当前版本，完整行在同版本优先：
argMax(tuple(current_team_id,status,assignee_id,created_at,updated_at,first_resolved_at),
       tuple(version,complete))
-- 首次时长总和 / 首次解决数：
sum(dateDiff('microsecond',created_time,first_resolved_time))/1000000.0/count()
-- 当前待处理积压，仅过滤最新状态：
current_status IN ('pending','in_progress')
~~~

正式统计一次 SQL 返回新增、首次解决的数量/总时长、积压和当前观测数量。Service 补日期零值并按总时长/总数量计算组织、日和团队均值；不会对每日平均再求平均。无首次解决样本时均值是 null，不用零秒替代无样本。

## 独立十工单验收

[RUNTIME] 在随机隔离 MySQL 和独立 CK 数据空间装载 10 工单、46 条显式历史。前 6 张在上线前存在，后 4 张在真实捕获运行时装载；重复导入与实时增量重叠后结果保持一致。Gold 的数值固定于测试输入文件，独立于正式统计 SQL。

| 北京时间日期 | 新增，预期/实际 | 首次解决，预期/实际 | 平均首次解决时长，预期/实际 |
| --- | --- | --- | --- |
| 2026-11-01 | 4 / 4 | 1 / 1 | 2 小时 / 2 小时 |
| 2026-11-02 | 3 / 3 | 2 / 2 | 12 小时 30 分 / 相同 |
| 2026-11-03 | 3 / 3 | 3 / 3 | 24 小时 40 分 / 相同 |

| 团队/边界对照 | 独立预期及实际 |
| --- | --- |
| 11/01 A 团队新增 | 3 |
| 11/02 首次解决 | A=1，B=1 |
| 11/03 首次解决 | A=2，B=1 |
| 当前待处理积压 | 全组织=5，A=1(T10)，B=4(T03/T04/T06/T08) |
| T01 转派 | 历史新增仍 A，首次解决 B |
| T04 重开/再次解决/当前转 B | 只算一次首次解决，历史归 A，当前积压归 B |
| T08 11/03 00:00:00 | 在 11/03 左边界计入，不计入 11/02 右边界 |
| 无历史事件的未来窗口 | 新增/解决=0、平均=null，当前积压仍=5 |

总首次解决为 6，平均 60600 秒（16 小时 50 分）；总当前未关闭为 8，与待处理积压 5 明确区分。团队 B 当前积压通过 Search 下钻核对的 ID 恰为 3、4、6、8。历史新增或首次解决不提供当前团队 Search 作为精确对应明细。

真实值、团队过滤、事件事实和最新快照保存在 [smoke-analytics-result.json](../artifacts/m3-01/smoke-analytics-result.json)，集成日志为 [integration-analytics.log](../artifacts/m3-01/integration-analytics.log)。原五张演示工单没有被固定样本覆盖。

## 初始化、确认与故障恢复

首次 CK 启动等待基础捕获快照就绪，在 MySQL 一致只读快照中按工单/版本读取全部历史，再发送权威当前工单。它使用同一 Kafka Topic，并在各分区写入初始化末尾标记，把每个分区下一 Offset 存为初始化完成条件。初始化发布一部分后失败可重跑；版本与业务事实键处理重复，没有全量重建业务库。

CK 采用自己的消费组，先写事实、再写当前快照，两者成功且 MySQL 处理记录成功才允许 Queue 提交 Offset。事实成功而当前失败时 Offset 停留，重放再次写两表；物理重复在查询归并。永久字段/协议问题保存隔离原文及原因，修复后按记录重投，恢复位置和原位置均可查询。

| 真正执行的场景 | 确认结果 |
| --- | --- |
| 创建与首次解决重复投递 | 只计一次 |
| 新版先到、旧版后到 | 当前状态保持新版 |
| 历史初始化与增量重叠、再次初始化 | 历史 46 个业务键，指标与 Gold 相同 |
| CK 当前表不可用，事实表已成功 | 事实物理行存在，Offset 不越过失败记录；恢复重放补齐 |
| 两表成功、提交前中断 | 真实 MySQL 处理记录触发器失败；Offset 未确认，重启产生物理重复而业务计数正确 |
| CK 永久字段故障 | 原文持久隔离，修复/重投后 recovered=1 |
| CK 暂停，ES 继续 | 两组 Offset 独立推进，CK 重启追赶 |
| CDC 重启 | 按保存检查点继续，ES/CK 最新版本恢复 |
| 原生 CK 停止/恢复 | MySQL 评论仍提交，统计返回 503，恢复后数值不变且确认追平 |

[RUNTIME] 最终 Kafka HTTP smoke 的原生 CK 停止到恢复测量为 35.22 秒，包含等待服务进程退出/数据目录释放及重启。早期 NATS 阶段更短的数字不代表最终 Kafka 环境。进程和查询失败已验证；突然断电、文件系统 fsync 及节点丢失不在本轮实验。当前 CK 使用同步 INSERT，fsync_after_insert 默认 0，不能据此宣称掉电持久性。

源端每两秒在 MySQL 提交一个水位标记，经同一 Binlog 读取后发送到所有 Kafka 分区。看板的 confirmed_through 是各分区已经提交标记的最小时间；只有初始化完成、最新标记已全确认、心跳新鲜、积压/处理中/重试/隔离均无未完成才声明 caught_up。页面 refreshed_at 独立表示这次请求时间。队列为空、页面刷新或最近一笔业务时间都不足以证明实时同步。

## API 与 Web

~~~http
GET /api/v1/analytics?start=2026-11-01&end=2026-11-04&team_id=10&team_id=20
~~~

[CODE] 当前 MySQL 有效 Supervisor/Admin 才可访问，组织范围来自服务端身份。日期必须是 YYYY-MM-DD，1–366 天；team_id 可重复 OR、最多 20 个且必须属当前组织。未知参数、重复单值或跨组织团队拒绝。完整响应/错误合同在 [api.md](api.md)，概览、每日、团队、有效/历史团队候选与同步信息来自同一接口。

Web 复用现有样式和登录，提供四张指标卡、趋势 SVG 与可读取日值表、团队积压、日期/团队筛选、刷新时间及水位。加载、零值、组织无数据、服务失败、同步落后分别呈现，团队文本安全转义。只有当前积压链接到当前团队和 pending/in_progress 搜索；历史指标没有误导下钻。ES/CK 可短暂不一致，页面明确说明当前 Search 明细在未追平时可能少于看板积压。

[RUNTIME] 本机 Chrome 两项看板测试通过：真实登录、Gold 指标、筛选、积压四张工单、服务停止、消费暂停、角色变化、特殊字符和基本响应式。最新截图可打开 [analytics-desktop-prd.png](../artifacts/m3-01/analytics-desktop-prd.png)，失败/滞后截图与 [浏览器结果](../artifacts/m3-01/browser-test-result.json) 同目录保存。这些是浏览器执行，未用 HTTP 测试替代。

## 运行、验证与文件入口

M2 Kafka 先按 README 初始化；开启已有分析只需：

~~~bash
make analytics-setup
make build
# 两个终端，先停止自己旧 sync/Web
make sync-analytics
make run-analytics
# 状态、重导入、隔离恢复
make analytics-status
python3 scripts/local_analytics.py snapshot
python3 scripts/local_analytics.py replay -partition=0 -offset=123
~~~

看板地址 http://127.0.0.1:18080/analytics；本地 supervisor/admin 密码为 TicketPulse!2026。示例隔离位置应替换为实际记录。配置来源 [sync.env.example](../configs/sync.env.example)；脚本安装校验 CK 发行包并只操作项目自有数据目录，已有数据不删除。测试夹具只清理自己随机数据库、Topic、ES 索引和 CK 空间。

| 验证命令 | 真实结果 |
| --- | --- |
| make test / make vet / make build | 退出 0 |
| go test -race ./... | 退出 0 |
| make integration | 56 项真实 MySQL 业务/身份回归，整套合计 76 项，退出 0 |
| make integration-search | 7 项 Kafka/ES/权限/恢复测试通过 |
| make integration-analytics | 2 项真实 MySQL/Kafka/CK/ES 综合测试通过，含 race |
| 五项 M1 smoke；make smoke-search | 全部通过 |
| make smoke-analytics | Gold、独立消费及原生 CK 停止/恢复通过 |
| make browser / browser-search / browser-analytics | Chrome 4 / 2 / 2 项通过，无跳过 |

[验证汇总](../artifacts/m2-02/verification-summary.json) 链接到各次真实命令、环境与日志。源码和测试已经修复测试发现的两类实际问题：CK 未知字段曾默认跳过，现已显式拒绝并列出 INSERT 字段；原生停止不能只等待端口关闭，现等到自有进程退出和数据目录释放。没有改动样本预期来获得通过。

| 源码/迁移 | 职责 |
| --- | --- |
| internal/syncpipe/analytics_snapshot.go | 一致快照读取原历史与当前行，统一 Topic 初始化 |
| internal/syncpipe/analytics_consumer.go | 历史匹配、两表写入、重试/隔离与提交前处理记录 |
| internal/syncpipe/analytics_status.go、watermark.go | 分区确认水位、初始化与失败状态 |
| internal/analytics/clickhouse.go、model.go、service.go | CK 协议、正式 SQL、窗口、指标聚合 |
| internal/mysqlstore/analytics.go、internal/httpapi/analytics.go | 当前主管授权和参数/错误 |
| web/app.js、app.css、embed.go | 看板、趋势、入口和错误交互 |
| db/migrations/007_analytics_sync.sql、008_kafka_sync.sql | 006 后新增分析和 Kafka 控制表；可重复执行，业务不重写 |
| tests/analytics、internal/mysqlstore/analytics_integration_test.go | 独立输入/Gold与真实故障 |
| scripts/local_analytics.py、analytics_*、tests/browser/analytics.spec.js | 原生 CK、隔离夹具、HTTP/Chrome |

M3 开始前的 M2 基线为 103 个文件，集合 SHA-256 8f750f71c6c41825ce178aa967a856f91c92404d28082bf8a9a487f6a949c9bc，恢复归档为 [m2-complete-source.tar.gz](../artifacts/m3-01/m2-complete-source.tar.gz)。本目录 [changes.patch](../artifacts/m3-01/changes.patch) 包含 M3 加追加 Kafka 迁移的最终累计差异，另有 before-Kafka 单独差异，避免把 Kafka 改动都算作分析实现。最终文件哈希/代码量见 [changes-manifest.json](../artifacts/m3-01/changes-manifest.json)；原演示当前与历史对账见 [demo-reconciliation.json](../artifacts/m3-01/demo-reconciliation.json)。

[INFER] 这一实现满足 PRD 的 M3 三指标、历史/当前归属、最低可靠性与 Web 验收范围。[UNKNOWN] 大数据下聚合成本、重复物理行清理、超大初始化、在线重分区、多节点 CK/Kafka 高可用、断电恢复和跨平台脚本尚未验证。初始化目前顺序发布，查询每次归并全部相关业务版本，未做物化预聚合或性能宣传；这些留到 M4/M5。数据库/集群更换需要明确重建流程，不能沿用旧进度却删除投影空间。
