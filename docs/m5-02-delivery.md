# TicketPulse M5-02 交付：十万级正式恢复与对账查询修复

2026-11-09。本轮回答十万工单的正式恢复为何超时，以及能否保留 M4 一致性协议完成验收。源码依据是本轮开始时的 live repository、M5-01 完整 Diff 与原始失败证据；改动前源码已保存到本轮基线归档。项目没有已提交的 Git 基线，完整 Diff 因此以该归档为起点。

## 验收结论

| 分类 | 判断 | 直接证据 |
| --- | --- | --- |
| RUNTIME | 十万工单正式恢复通过；实际 SIGKILL 后续跑，任务 completed，报告 consistent，差异 0 | [正式恢复结果](../artifacts/m5-02/recovery/resume/status.json) |
| RUNTIME | 最终新增写入后的再检查仍通过：100,212 工单、975,735 历史；独立全量内容核对也一致 | [最终任务](../artifacts/m5-02/recovery/final-status.json)、[独立核对](../artifacts/m5-02/independent-final-audit.json) |
| RUNTIME | 原历史分页每页扫描 975,303 行，中间页排序 487,507 行；优化后索引范围读取 500 行，无排序 | [原计划](../artifacts/m5-02/sql/reasonable-original/middle.explain.txt)、[正式 SQL 计划](../artifacts/m5-02/sql/production-query-plans/middle.txt) |
| RUNTIME | 同资源、同索引三轮比较，中间页中位数 308.188 → 24.905 ms，末页 568.617 → 17.478 ms；内容哈希一致 | [重复 A/B](../artifacts/m5-02/sql/repeated-ab/results.json)、[汇总](../artifacts/m5-02/sql/ab-summary.json) |
| CODE | 保留原 RR 一致读视图、全组织内容检查、版本连续性、历史口径与四种状态；没有放宽超时或删减检查 | [完整 Diff](../artifacts/m5-02/full.diff) |
| RUNTIME / UNKNOWN | 搜索补充实验未复现 SEARCH_UNAVAILABLE；旧请求缺少底层错误链，唯一原因仍未知 | [搜索证据](../artifacts/m5-02/search/summary.json) |

CODE 表示源码可直接确认；RUNTIME 表示本机真实组件实验；INFER 表示证据支持的解释或建议；UNKNOWN 表示尚未取得足够证据。表中的性能只适用于本次本机数据与配置，不能外推生产集群容量。

## 隔离环境与同一份输入

本轮复制 M5-01 已有随机测试库的业务表，采用事务一致 mysqldump，保留原数据分布、随机种子 20261109 和之前真实 HTTP 产生的业务版本。初始输入为 100,000 工单、975,303 历史、265,000 评论。未复制旧恢复控制表，新的冻结和进度由正式恢复程序创建。SQL 导入和夹具预装载用于准备输入，不计为业务 API 吞吐或正式恢复吞吐。

源实例、应用和候选资源如下；原演示 MySQL 33306、Web 18080、CK 18123 始终保留。

| 组件 | 本轮资源 | 隔离条件 |
| --- | --- | --- |
| MySQL 8.4.5 | 33307；ticketpulse_test_m502_d0396684dc | 独立 datadir、socket、进程所有权标记；UUID 55ca2c86-dbf9-11f1-96eb-bbdb1aec55e4 |
| CDC | 复制连接 33307；server ID 350207 | 独立捕获检查点；数据库 server ID 502 |
| Kafka 4.4.0 | 19092；该随机库命名的 Topic，三分区 | 独立 Topic、消费组和任务 Offset；共享服务未停止 |
| ES 8.17.5 | 19200；随机测试及 recovery100k 候选索引 | 只向该库的独立索引写入 |
| ClickHouse | 18124；随机测试及 recovery100k 候选空间 | 独立于原 18123；4 GiB 服务预算、512 MiB 单查询预算、2 查询线程 |
| 测试应用 | 18085 | 本轮独立实例；真实 HTTP、认证、MySQL 权限复核 |

主机沿用 M5-01 的 Apple M5 Pro、18 个逻辑 CPU、48 GiB 内存、macOS arm64；硬件资料见 [M5-01 硬件](../artifacts/m5-01/environment/hardware.json)。本轮没有百万档或持续极限负载。单次正式 HTTP 采样为 10～20 秒，恢复脚本单次进程限 1,200 秒，对账等待沿用 M5-01 的 5 分钟，MySQL 驱动读超时仍为 10 秒，应用连接池仍为 20。冻结和回填期间采集活跃事务及 History list length，原始序列保存在进度文件；没有据此声称已测得完整 Undo 字节曲线。

资源对比先在独立实例执行原 SQL，再重启自己的实例换配置。三组使用同一份静止业务数据。

| 配置项 | 原资源档 | 合理资源档 |
| --- | --- | --- |
| Buffer Pool | 64 MiB | 2 GiB |
| max_connections | 64 | 128 |
| table_open_cache / definition_cache | 4,000 / 2,000 | 512 / 512 |
| 实际 open_files_limit | 65,536 | 65,536 |
| ROW / FULL Binlog | 保持 | 保持 |

两档都显式提高了进程文件句柄上限，原资源档没有复现 M5-01 的低句柄条件。资源结果是整组配置的对比，主要解释内存缓存改善；不能分别归功于每个参数，也不能宣称本轮定位了旧句柄耗尽的全部影响。实际启动命令、有效值和隔离身份见 [原资源](../artifacts/m5-02/environment/baseline-mysql.json)、[合理资源](../artifacts/m5-02/environment/reasonable-mysql.json)、[复制说明](../artifacts/m5-02/environment/source-clone.json)。复制说明的计数采样发生在冻结期 HTTP 写入后，975,413 不是初始历史量。

## 为什么独立审计能成功，正式对账却超时

CODE：正式对账先开启一份只读 RR 事务，完整读取源摘要，在同一视图中比较 ES、CK 当前状态及全部历史，并核对统计窗口。随后开启第二份 RR 事务重新读取源摘要，识别检查期间发生的写入。数据页按实际返回的最后一行推进组织、工单和版本游标。历史有三次完整扫描，分别服务源摘要、CK 内容比较和后置源摘要；这些是已有正确性检查，不因重复次数而直接判为冗余。

RUNTIME：M5-01 失败证据显示，正式检查已记录当前工单 100,000，历史读取尚为 0，随后 MySQL 驱动报 socket i/o timeout / invalid connection。发生位置落在前置源历史扫描。旧证据没有保存那一次失败的精确 SQL 参数、计划及缓存状态，因此不能精确重演该次十秒超时，也不将本轮几秒的暖缓存样本冒充同一次失败。原证据见 [M5-01 恢复状态](../artifacts/m5-01/m4-100000-resume/final-status.json)。

原历史 SQL 的筛选、排序如下，完整 SELECT、连接条件和实际参数保存在对应 .sql 文件中：

~~~sql
FROM ticket_histories h JOIN tickets t
  ON t.id=h.ticket_id AND t.organization_id=h.organization_id
WHERE (h.organization_id,h.ticket_id,h.version) > (org,id,version)
ORDER BY h.organization_id,h.ticket_id,h.version LIMIT 500
~~~

RUNTIME：首批、中间批、末批和空尾批的执行计划都明确出现历史全表扫描。它先遍历 975,303 条历史，再筛选游标之后的行、排序、限制本页并连接工单。首批排序 975,303 行，中间批排序 487,507 行；即使尾部只返回 102 行或 0 行，仍扫描整张表。每次 Handler_read_rnd_next 增量为 975,304，Sort_scan 增量为 1。Created_tmp_tables 和 Created_tmp_disk_tables 为 0，只能说明没有这两类临时表，不能据此否认计划中的 filesort。

INFER：缺少按组织、工单、版本排列的历史索引，以及元组比较未形成有效范围起点，是本轮证实的高查询成本来源。64 MiB 缓存与大表、其他负载叠加，更容易触发单次读取超时；单页扫描排序的累计成本也威胁完整检查的五分钟预算。独立审计用另一条有序流式 SQL 一次读取全量数据，路径不同，因此其成功不意味着正式分页对账本来可完成。

## 执行计划与 A/B

以下耗时为 mysql 客户端启动、查询、结果传输的墙钟时间，位于 EXPLAIN ANALYZE 后的采样；不是纯服务端执行时间，也不是冷缓存比较。每页返回值的哈希在三组中一致。第三组是早期强制连接顺序的优化实验，含 STRAIGHT_JOIN；最终普通 JOIN 的收益由后面的同资源、同索引三轮 A/B 单独验证。

| 游标 / 返回行数 | 原 SQL + 64 MiB（秒） | 原 SQL + 2 GiB（秒） | 早期优化实验 + 2 GiB（秒） |
| --- | --- | --- | --- |
| (0,0,0) / 500 | 4.601915 | 3.114695 | 0.023539 |
| (1,50000,7) / 500 | 1.514363 | 1.318020 | 0.023354 |
| (1,99990,7) / 102 | 0.504837 | 0.224872 | 0.014369 |
| (1,100000,999) / 0 | 0.496552 | 0.221032 | 0.011708 |

资源配置降低了部分墙钟时间，但没有改变原 SQL 全表扫描及排序。新增复合索引以后，元组写法仍可能从索引头部过滤到游标；仅把 WHERE 展开而不指定索引，也曾选择连接后排序约 48.7 万行。为区分这些因素，本轮保存了 [备选计划](../artifacts/m5-02/sql/alternatives/)，最终保留展开的有序 Keyset 条件和匹配索引，不保留实验中的 STRAIGHT_JOIN。

CODE：历史新增索引为 (organization_id,ticket_id,version)。现有工单索引 uk_tickets_org_id 足以支持当前状态分页，工单表没有新增索引。正式历史查询为：

~~~sql
SELECT h.id,h.organization_id,h.ticket_id,h.action,h.operator_id,h.version,
       h.before_state,h.after_state,h.responsible_team_id,h.occurred_at,
       h.reason,h.comment_id,t.ticket_no,t.created_at
FROM ticket_histories h FORCE INDEX (idx_history_reconcile)
JOIN tickets t ON t.id=h.ticket_id AND t.organization_id=h.organization_id
WHERE (?=0 OR h.organization_id=?)
  AND (h.organization_id>?
    OR (h.organization_id=? AND
       (h.ticket_id>? OR (h.ticket_id=? AND h.version>?))))
ORDER BY h.organization_id,h.ticket_id,h.version LIMIT ?
~~~

RUNTIME：优化计划是真正的索引范围扫描，读满页 500 行即停止，末页 102 行、空页 0 行；Sort_scan、Sort_rows 和随机扫描增量为 0，满页 Handler_read_next 为 499。正式中间页的 EXPLAIN ANALYZE 显示连接输出 500 行约 0.632 ms，而 Go 页记录含传输和 Scan 为 4.154 ms，两者口径不同。

同资源、同索引、相同游标再交替测量三轮，消除“只增加索引”与“改变游标表达式”的混淆。下表为墙钟中位数；每轮内容哈希均相同。

| 页 | 原元组 SQL，索引已存在（ms） | 最终优化 SQL（ms） | 墙钟比值 |
| --- | --- | --- | --- |
| 首页 | 25.486 | 23.592 | 1.08 |
| 中间页 | 308.188 | 24.905 | 12.37 |
| 末页 | 568.617 | 17.478 | 32.53 |
| 空尾页 | 568.479 | 15.584 | 36.48 |

当前工单 SQL 也改为展开的有序游标条件并使用现有匹配索引。末页原计划从头读取的服务端实际时间约 59 ms，优化约 0.34 ms；空尾页优化约 0.008 ms。证据见 [当前状态计划](../artifacts/m5-02/sql/current-pages/)。该改动与历史修复属于同一分页问题，没有改变业务查询合同。

## 有限代码改动与迁移

| 文件 / 入口 | 改动 |
| --- | --- |
| [reconcile.go](../internal/syncpipe/reconcile.go) | 当前和历史分页使用有序范围条件；可选逐页观察器记录真实 SQL、参数、游标、行数、耗时及错误 |
| [010_reconcile_index.sql](../db/migrations/010_reconcile_index.sql)、[schema.sql](../db/schema.sql)、[embed.go](../db/embed.go) | 新库索引与已有库显式追加迁移 |
| [rebuild.go](../internal/syncpipe/rebuild.go)、[rebuild_check.go](../internal/syncpipe/rebuild_check.go)、[CLI](../cmd/ticketpulse-rebuild/main.go) | migrate 安装索引；run/check 只读已有结构；透传观察器，新增默认关闭的 page-trace |
| 两个真实 MySQL 回归入口 | 允许本项目独立 33307/socket，同时继续校验服务端 UUID |
| bench 应用与 prepare | 接受本轮随机测试库前缀；可选诊断日志，不改产品搜索错误语义 |
| m502 系列脚本 | 独立实例管理、计划采样、正式恢复、独立审计及搜索诊断 |

CODE：主检查仍使用同一 RR 视图，源前后摘要、全组织覆盖、历史从版本 1 开始的连续性、前后状态、事件团队、首次解决、缺失与额外键及冲突事实检测都保留。组织 1 参数只选择附加统计窗口，正式任务的内容检查仍为全库，最终报告 organization_id 为 0。consistent / catching_up / inconsistent / uncheckable 状态判定未放宽。

逐页证据默认不生成；显式开启时采用新建文件、权限 0600，已有文件拒绝覆盖，写失败会使 CLI 报错。它观察查询，不改变分页结果或任务完成条件。新索引仅通过 migrate 或新库 schema 安装；原演示库本轮未安装 010。已有库运行新版恢复之前需显式迁移，建索引的资源窗口和大表写入开销应单独评估。本轮随机库安装该索引用时约 0.522 秒，没有测量生产在线 DDL 或索引写放大。

## 十万级正式恢复时间线

正式任务 recovery100k 使用原 Rebuilder 冻结、回填、Kafka 接续和完整 Reconcile，没有用独立审计代替任务验收。初始冻结输入共 1,075,303 项；任务自己的 ES/CK 候选与运行中测试消费组并行，业务写入继续经真实 HTTP 发生。

| 阶段 | 实测 | 结果 / 时长口径 |
| --- | --- | --- |
| 完整冻结 | 输入 100,000 当前 + 975,303 历史 | 首次观察到 frozen 在启动后 141.454 秒；是提交可见的观测上界，非精确事务计时 |
| 首次回填及 SIGKILL | 进程 exit -9 | 运行 157.769 秒；持久游标 63,000；不是正常取消 |
| 同任务恢复回填 | 同一输入 SHA256，attempts 2 | 恢复启动后约 101.816 秒观察到全部 1,075,303 项已应用并进入 catchup |
| Kafka 接续 | 三分区屏障及任务 Offset 持久化 | 恢复启动后约 107.919 秒观察到 checking；约 6 秒为轮询间隔的阶段差，不是精确 Kafka 独占耗时 |
| 首次正式完整对账 | consistent，difference_count 0 | 报告 05:04:11.114613Z～05:07:16.915748Z，185.801 秒 |
| 整个恢复进程 | exit 0，completed | 293.922 秒，包含剩余回填、追平和正式检查 |
| 首次独立全量审计 | 三组内容哈希相同 | 40.452 秒 |
| 附带搜索写入后的正式再检查 | completed / consistent，差异 0 | 177.463 秒；仍是完整检查 |
| 最终独立全量审计 | 三组内容哈希相同 | 38.782 秒 |

时间线日志使用 UTC；原始采样、控制表日志、命令及退出码见 [timeline](../artifacts/m5-02/recovery/timeline.json)、[阶段汇总](../artifacts/m5-02/recovery/stage-summary.json)、[杀进程](../artifacts/m5-02/recovery/kill/)、[恢复](../artifacts/m5-02/recovery/resume/)。冻结 SHA256 为 50ad45eed5ce32519896b33f30cf608b239e4a9da6c99e7cd6500eac6faa4085，实际全部回填后才接续增量。报告中的 caller_confirmed 来自既有任务 catchup 回调，不是人工传入成功；三分区屏障详情保存在任务日志，不能用 Offset 总和替代连续水位。

冻结和恢复分别执行 20 秒、10 并发的限速读写，正式采样各 200 请求，其中各 100 写请求，约 5 TPS；错误及超时均为 0，p95 分别 8.446 / 5.430 ms。预热记录独立保存，两段全部业务版本新增 220 条历史。这个负载验证恢复期间的新版本接续，不作为高并发容量证明。输入、逐请求结果及认证后的实际操作见 [冻结期 HTTP](../artifacts/m5-02/http/during-freeze/)、[恢复期 HTTP](../artifacts/m5-02/http/during-resume/)。

正式历史扫描每个阶段 1,953 页、975,523 行；源前扫描、CK 比较扫描、源后扫描的查询及 Scan 累计分别 18.433、18.184、27.238 秒，最大页分别 271.998、218.472、1,604.243 ms，全部没有页错误。当前工单累计 1,206 页、600,000 行约 1.530 秒。当前工单计数包含同一阶段的内容和统计独立遍历，所以汇总中游标重置不等于单次分页回退。正式首、中、倒数第二、末和空尾页的准确 SQL/参数及计划见 [生产查询计划](../artifacts/m5-02/sql/production-query-plans/)。页数、游标和累计页读取时间见 [逐页进度](../artifacts/m5-02/recovery/page-progress.jsonl)，累计读取成本见 [逐页汇总](../artifacts/m5-02/recovery/page-summary.json)、[成本合计](../artifacts/m5-02/recovery/page-cost-totals.json)。185.801 秒还包含 CK 内容分页、Go 历史校验和摘要等工作，不能全归为 MySQL 查询时间。

## 独立全量核对与历史正确性

RUNTIME：第一次正式恢复完成时为 100,000 工单、975,523 历史，首次解决 40,010、重开 30,250。随后搜索补充实验含预热又创建 212 工单，正式再检查接续到 100,212 工单、975,735 历史，源前后摘要相同、全部投影一致。最终独立审计在业务负载停止后逐行检查内容，不调用正式 Reconcile 的比较函数，不仅比较总数。

| 全量对象 | MySQL 行数 | 下游规范行数 | 相同内容 SHA256 |
| --- | --- | --- | --- |
| ES 当前字段及 external version | 100,212 | 100,212 | ae03c347151a648377a19c9020ddbecc80a2b87c0d194e40cd8ffcf930df4736 |
| CK 全部业务事实 | 975,735 | 975,735 | 0af4ee99ce8e0867e7658c7257e7419b3cf65b3e6a0fbbffffb4695866c0c400 |
| CK 最新当前状态 | 100,212 | 100,212 | 70d88c78796142035521d5b773a1828e648a95412bd6eac8b25e2bf8a54ee8c5 |

独立审计覆盖 ES 全部投影字段、版本，CK 当前状态及每条历史的事件键、动作、发生时间、事件团队、前后团队和状态、负责人、初始团队与首次解决时间；SQL 原文在 [最终独立审计](../artifacts/m5-02/independent-final-audit.json)。它与正式报告的序列化口径不同，各系统的正式摘要字符串不要求彼此相同，正式结果来自字段比较；独立规范化哈希则按同一格式相等。

CK 最终物理历史 976,235，规范事实 975,735，正好多出 500 条相同重放；物理状态 1,075,735 属于正常各历史版本状态及重放，不是 100,212 个当前对象的重复业务计数。独立审计按组织、工单、版本读取每一物理行，对相同键比较全部业务字段，只对完全相同重复行计一次，任意冲突即失败。正式冲突键为 0，首次解决和重开均与 MySQL 相同；独立首次解决团队计数为 28,010 / 8,000 / 4,000，与 CK 一致，见 [团队核对](../artifacts/m5-02/independent-first-resolution-team.json)。历史来源仍是已持久化业务事实，没有由当前状态反推历史。

## 故障及回归

真实 MySQL、Kafka、ES、CK 回归命令为：

~~~bash
go test -race -tags=integration,m2,m3,m4 -count=1 -v ./internal/mysqlstore ./internal/syncpipe
~~~

RUNTIME：共 84 PASS、0 FAIL，其中原 M4 十个故障场景全部通过，整体墙钟 136.025 秒。覆盖隔离目标恢复、并发版本与续跑、Kafka 保留损失、真实 CLI 强杀、投影已写而游标未提交、延迟/超时/损坏、持续写入与 Topic 身份变化、已完成目标被删除、冻结重叠区间过期，以及跨组织内容不能被统计参数隐藏。M1 事务/权限/HTTP/登录，M2 真实 CDC/独立组/部分发布，M3 十工单 PRD 固定样本与首次解决团队口径均在原回归中通过。测试子进程使用本轮 race 构建，未用跳过测试代替真实组件。

日志见 [integration.log](../artifacts/m5-02/regression/integration.log)、[逐用例汇总](../artifacts/m5-02/regression/test-summary.json)、[进程结果](../artifacts/m5-02/regression/result.json)。全项目单测、重点 race 和 vet 通过，见 [最终单测](../artifacts/m5-02/final-unit.log)、[race](../artifacts/m5-02/unit-race.log)、[最终 vet](../artifacts/m5-02/final-vet.log)。本轮没有新增业务 UI，不声称重新完成全部 Chrome 页面验收。

## SEARCH_UNAVAILABLE 附带调查

RUNTIME：主目标通过后，独立应用开启诊断日志，并用只读 ES 代理记录 HTTP 状态、分片失败、超时、候选字段与传输异常。200 并发团队过滤正式 3,247 请求全部成功，307.409 QPS，p95 1,259.036 ms；50 并发直连混合读写 6,781 请求全部成功，671.256 QPS、17.620 写 TPS，p95 173.375 ms。两段正式入场窗口各 10 秒、预热独立，代理错误记录 0；这些短实验只用于故障复现，不建立稳定并发上限。

CODE：MySQL 权限与新鲜度复核失败返回原错误，映射为 INTERNAL_ERROR / REQUEST_TIMEOUT；授权结果不足可返回短页；候选预算耗尽为 SEARCH_TOO_BROAD；ES deadline 为 SEARCH_TIMEOUT。ES 非 deadline 的传输、非成功 HTTP、响应解码、分片失败或候选缺失/非法字段会进入 SEARCH_UNAVAILABLE。现有候选入口将这些具体底层原因合并，旧 M5-01 应用也没有记录逐请求完整错误链。因此不能把旧 SEARCH_UNAVAILABLE 归为 MySQL 权限、候选预算或 ES 容量极限。

UNKNOWN：旧故障本轮未复现，无法从调用栈恢复当时唯一原因。本轮保留 [原始请求](../artifacts/m5-02/search/)、[分类与结果](../artifacts/m5-02/search/summary.json)，未实施搜索重构或声称修复。下一步宜先保留服务器内部错误分类及原因，保持外部 API 合同；CK 50 并发内存问题留待独立专题。

## 失败实验、边界与下一步

初次克隆后的结构初始化因 SQL 拼接换行错误失败，修正命令后完成；原日志保留。初次独立审计直接把 CK 物理行当业务事实，因 500 条相同重放得出不相等；后续一次全量规范聚合遇到 CK HTTP 500。最终改用独立有序流式比较每一物理行，检查冲突并规范去重，且正式任务自身已先取得 consistent。失败输出见 [复制日志](../artifacts/m5-02/source-clone.log)、[物理审计](../artifacts/m5-02/independent-physical-audit.json)、[聚合失败](../artifacts/m5-02/independent-full-audit-canonical.log)。

复用旧 CK 启动脚本曾覆写旧 startup.json，本轮已按原 SHA256 精确恢复原字节，新记录迁入本轮目录。新增 m502_ck 包装入口将未来复现证据写入本轮带时间目录，避免再次覆盖旧记录。保存说明见 [证据恢复说明](../artifacts/m5-02/environment/previous-evidence-note.json)。最终检查 1,191 份旧证据，变化为 0；演示 MySQL 所有基线业务表、ES 文档及 external version、CK 所有物理行均与基线一致。

INFER：查询成本已经下降，但冻结长事务仍约 141 秒，单任务冻结控制表实际 data_length 为 3,329,228,800 字节（约 3.10 GiB）；历史表数据约 1.75 GB、全部索引约 139 MB。完整对账仍约三分钟，下一轮应先分析剩余 CK 分页、Go 校验和摘要的耗时分布，再决定范围有限的优化。冻结协议改造需要提出等价 RR/水位保证和证据，等待用户决定；本轮保持协议。

UNKNOWN：新增索引的持续写入开销、更大规模恢复、持续高并发下对账预算、旧搜索唯一根因、多节点 HA 和生产在线 DDL 尚未验证。断点本身没有改变 Kafka 重叠保留的要求：冻结后的必需区间已丢失、目标身份变化、源事实破坏或索引迁移部分失败仍需人工核验及新建任务，不能忽略失败继续标记完成。

本轮测试应用、独立 MySQL、CK 18124 和诊断代理均已停止；数据、候选、Topic、冻结输入和日志保留。原演示服务仍可达。保护核对见 [演示完整性](../artifacts/m5-02/environment/demo-preservation-baseline.json)、[旧证据完整性](../artifacts/m5-02/previous-evidence-preservation.json)、[最终端口](../artifacts/m5-02/environment/final-ports.json)。未暂存、提交或推送。

## 交付索引与四个最终回答

| 问题 | 回答 |
| --- | --- |
| 十万对账为什么超时？ | 原证据在前置历史读取报 MySQL socket 超时。本轮计划证实每页全表扫描排序，缓存配置只能降低部分时间，不能消除高成本。旧失败精确游标与当时负载未记录，保留这一归因边界。 |
| 修复了什么？ | 历史增加匹配的组织/工单/版本索引，当前和历史展开有序范围分页并显式选匹配索引；添加默认关闭的真实页诊断。没有缩小检查或增加超时。 |
| 正式恢复是否通过？ | 通过。十万冻结、ES/CK 回填、Kafka 接续、实际 SIGKILL 续跑后 completed / consistent，差异 0；额外写入后再检查和独立全量核对仍通过。 |
| 是否真正降低查询成本？ | 是。相同资源、相同索引的重复 A/B 及实际扫描计数证明末段从头扫描消失；中间页墙钟中位数约降 12.37 倍，末页约降 32.53 倍，内容哈希一致。没有把资源增配收益全部算成代码收益。 |

复现入口为 [reproduce.md](../artifacts/m5-02/reproduce.md)。完整源码改动见 [full.diff](../artifacts/m5-02/full.diff)，逐文件尺寸及前后哈希见 [file-manifest.json](../artifacts/m5-02/file-manifest.json)；Diff 从只读归档应用到临时目录后逐字节复核见 [diff-verification.json](../artifacts/m5-02/diff-verification.json)。全部证据清单见 [artifact-manifest.json](../artifacts/m5-02/artifact-manifest.json)，结构化结论、失败实验及未解决问题见 [conclusions.json](../artifacts/m5-02/conclusions.json)、[failures.json](../artifacts/m5-02/failures.json)、[remaining-risks.json](../artifacts/m5-02/remaining-risks.json)。最终任务、保护条件、端口和未暂存检查见 [closeout.json](../artifacts/m5-02/closeout.json)。
