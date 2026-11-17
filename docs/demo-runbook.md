# TicketPulse 产品演示手册

这条流程在独立小环境中展示工单协作、异步搜索和运营分析，再展示候选重建、受控推广与回滚。演示期间 MySQL 始终是业务真相源，A/B 是同一业务源的两套 ES/CK 投影。切换接受明确维护窗口，保证范围为 macOS 同机、可枚举且无自动重启的进程。

[RUNTIME] DEMO-01 已通过真实 Chrome、MySQL、Kafka、ES、CK 和官方 CLI 的往返预演。最终 A3 服务 14 张工单、61 条规范历史，A/B 完整对账差异及 CK 冲突均为 0。浏览器会话跨两次 API 重启保留，旧 A1/B2 游标在 A3 返回 409。原始结果与已复现体验问题见 [本轮预演记录](../artifacts/demo-01/20261116T102333.326700Z/demo-report.md)。

## 先选择演示方式

日常面试先展示普通业务，再用终端中的修订号、标记和对账解释 M4。建议给普通业务留 5–8 分钟，技术往返留 5–10 分钟；这是演示安排，不是恢复时长 SLA。真实执行遇到未就绪或对账失败必须停止推进。

当前原演示 18080 和保留 A3 60555 不属于这份手册的操作目标。DEMO-01 私有 API 使用 **http://127.0.0.1:18101**，这是该夹具的固定端口，不是当前在线承诺。Release-01 的只读观察确认 60555、18101 当时没有在线 API；不能直接按历史完成记录继续演示。已有 DEMO-01 记录不能重新初始化成 A1；完整 A→B→A 应在另一干净 checkout 中建立新私有运行，并先确认全部端口空闲，不删除现有标记或重置 Offset。仅做当前源码的普通功能验收，使用 README 的独立最小启动路径。

## 环境准备

### 依赖与资源

| 组件 | 本轮验证版本／地址 | 职责与成功判据 |
| --- | --- | --- |
| macOS arm64 | 本机；进程观察使用 ps、lsof、launchctl | 受控 API 真实退出可核验；其他平台未验收这套排空协议 |
| Go | 1.27.1；模块要求 1.24+ | 编译官方 API、同步、重建和控制 CLI |
| Python | 3.9.6 | 原生发行包、私有环境与命令包装 |
| Node／Chrome／Playwright | Node 20.20.2、Chrome 149、Playwright 1.65.0 | 仅用于浏览器预演；产品本身无需 Node 或前端开发服务 |
| MySQL | 8.4.5；33316，独立 socket | ROW/FULL Binlog、UUID 和独立数据目录正确 |
| Kafka | 4.4.0，Temurin 21.0.13+9；19110／Controller 19111 | 单节点 KRaft，三分区，复制 1／最小 ISR 1；本轮不演示 Broker HA |
| Elasticsearch | 8.17.5；HTTP 19210／transport 19312 | 单节点健康 yellow/green；索引 UUID 可读取 |
| ClickHouse | 26.10.2.103；HTTP 18133／native 19011 | Atomic 空间，两张表 UUID 可读取 |
| API | 18101 | 内嵌 Web；readyz 200 且目标身份与持久选择一致 |

资源名称由新运行随机生成：MySQL 为 `ticketpulse_test_demo_<random>`，Topic 为 `ticketpulse.<schema>.tickets.v1`，A 为 `<schema>_tickets_v1` 与 `<schema>_analytics_v1`。重建 CLI 为 B 生成新的 `<schema>_rebuild_demo_rebuild*`。消费组 A 用既有 `.es.v1/.ck.v1`，B 用 `.es.target.candidate_v1/.ck.target.candidate_v1`。

本轮新增的小脚本只负责私有环境、环境参数和直接父进程。所有冻结、证明、交接、提交、观察与恢复仍由既有 `ticketpulse-control` 执行；没有新的切换控制器。`m403c3_suite.py` 是自动故障验收入口，含固定 C3 地址和大量测试；`m403c3_live.py` 会先构建/交接，再完成自动往返，不适合作为逐步手工演示入口。本手册复用其已验证的发行包及进程核验函数，并在调用前改成 DEMO-01 的所有路径和端口。

### 从干净目录启动

以下命令均在项目根目录执行。先确认上表端口空闲、无同名 DEMO 标记；有未知进程、资源归属不明或自动重启入口就停止。预计私有运行约 1 GiB 磁盘、2 GiB RSS；脚本硬边界为磁盘和所属进程 RSS 各不超过 8 GiB、可用磁盘不少于 20 GiB。本轮没有完整重验发行包的外网下载；使用已有校验过的版本，下载路径是源码支持项。

~~~bash
python3 -B scripts/demo_environment.py packages
python3 -B scripts/demo_environment.py new
python3 -B scripts/demo_environment.py start
python3 -B scripts/demo_environment.py verify
python3 -B scripts/demo_steps.py build
python3 -B scripts/demo_steps.py install
python3 -B scripts/demo_steps.py workers-a
~~~

`packages` 只准备校验过的发行包，不调用原演示 setup。`new` 创建时间戳目录和本轮标记；已有 `.local/demo-01/current.json` 时明确拒绝覆盖。`start` 核对独立服务后创建新 schema、合成用户和十工单 Gold，再生成私有密码。失败后保留所有目录和日志，先检查实际状态，不重复覆盖初始化。

记录本轮路径，后续只使用非凭据变量：

~~~bash
DEMO_RUNTIME=$(jq -r .runtime .local/demo-01/current.json)
DEMO_EVIDENCE=$(jq -r .evidence .local/demo-01/current.json)
~~~

### 初始化与迁移的准确顺序

| 次序 | 输入／官方命令 | 说明 |
| --- | --- | --- |
| 1 | schema.sql → seed.sql → login_seed.sql | 新库基线已含 M1 当前结构；不重跑 001–005。seed 建合成用户/团队/成员，login seed 补用户名 |
| 2 | 006 → 007 → 008 | 搜索状态、分析状态、Kafka 检查点/投递/标记表 |
| 3 | 十工单 Gold，只在新库导入 | 独立冻结 PRD 样本；新 HTTP 创建从 ID 100 起，保留样本 |
| 4 | ticketpulse-rebuild -mode=migrate | **009 + 010**；检查后补历史有序对账索引 |
| 5 | control -mode=migrate | 011 目标合同和业务闸门 |
| 6 | control -mode=migrate-handoff | 012 候选交接和版本化状态 |
| 7 | control -mode=migrate-read-control | 013 读选择与操作记录，安装不等于启用 |
| 8 | control -mode=migrate-read-runtime | 014 API boot/排空/就绪证据 |
| 9 | control -mode=migrate-read-rollback | 015 反向引用及 legacy writer 进程观察；必须先安装再启动 A 消费者 |
| 10 | freeze → enroll-existing A → read-init A → resume | 正式初始化读选择 revision 1，必须核验源、Topic、ES、CK 和完整事实 |

[CODE] `demo_steps.py install` 顺序调用 4–9 的官方命令。README 原来把 `local_rebuild.py migrate` 说成包含 010，与当前源码不符；该包装仍只做 009，README 已改用真正安装 009+010 的 CLI。已有库不能导入 schema 或样本，不能直接重跑含 ALTER 的原 SQL，使用显式迁移接口并检查实际结构。

### 环境变量与启动职责

私有文件 `$DEMO_RUNTIME/environment.json` 由脚本加载，不需要在终端打印或逐个导出。文件权限为 0600。`login.json` 保存本轮随机浏览器密码；由操作者私下读取输入，不能投屏、截屏或复制到报告。DEMO-02 已移除页面的固定密码提示，实际认证仍使用初始化的私有密码。

| 参数 | 用途 |
| --- | --- |
| TICKETPULSE_DSN | API 的同一 MySQL 业务源及会话 |
| TICKETPULSE_SYNC_DSN／CONTROL_DSN | 同步／操作者连接；正式部署需各自最小权限，本私有夹具使用独立 socket root |
| CDC_ADDR／USER／PASSWORD | 本轮复制账号；仅访问本轮 MySQL |
| KAFKA_BROKERS | 本轮 Broker；Topic 名从 schema 生成 |
| ES_URL／SEARCH_INDEX、CK_URL／CK_DATABASE | 实际物理目标的预期配置；API 的权威选择来自 MySQL，配置不一致拒绝 |
| TARGET_VERSION | API 当前 A/B 的预期；A 的登记 ACTIVE 与 B 的登记 PREPARED 不被读推广改写 |
| REQUIRE_WRITE_GATE=true | 业务事务必须遵守持久闸门；CDC、内部状态和标记仍可写 |
| CURSOR_KEY | 32 字节稳定密钥；重启保持同值，不能依赖随机换密钥让游标失效 |
| READ_PROBE_TOKEN／READ_PERMISSION_TOKEN | 控制器真实就绪和授权探针，包装从合成 seed 取得，不写报告 |

启动顺序是数据库/消息/物理投影服务 → 迁移 → A 的 capture、consume、ck 三进程 → A 合同与读选择 → 直接 launcher → API。候选完成后才启动两个 B 标准消费进程。A/B 和 CDC 在读切换时持续运行，只排空 API。

先确认 A 消费正常：

~~~bash
python3 -B scripts/demo_steps.py sync -mode=status
python3 -B scripts/demo_steps.py sync -mode=analytics-status
~~~

要求 CDC checkpoint bootstrap_complete=true，ES/CK worker running，每分区有实际已提交位置；分析 bootstrap 已完成，无待处理/隔离。初次启动太早查状态可能失败，等待真实 worker ready 后重查。lag=0 仅是观察信息，A 登记及每次切换仍需全量对账与精确新标记。

初始化 A，再启动 Web：

~~~bash
python3 -B scripts/demo_steps.py control -mode=freeze -reason="DEMO initial A enrollment" > "$DEMO_RUNTIME/initial-freeze.json"
python3 -B scripts/demo_steps.py control -mode=enroll-existing -version=active_original -wait=60s
python3 -B scripts/demo_steps.py control -mode=read-init -version=active_original -wait=60s
python3 -B scripts/demo_steps.py control -mode=resume -freeze-id="$(jq -r .freeze_id "$DEMO_RUNTIME/initial-freeze.json")"
python3 -B scripts/demo_steps.py launcher
python3 -B scripts/demo_steps.py api-start --version A
curl --fail --silent http://127.0.0.1:18101/readyz
~~~

成功时读选择为 active_original/revision 1，API 有真实 boot/PID/父进程/监听，readyz 200。launcher 只接受本机 0600 Unix socket 的显式 API start/stop/status，不自动重启，不操作读选择。其哈希、官方二进制哈希与本轮独占目录形成排空清单；运行期间禁止重新构建覆盖这些二进制。

## 普通产品演示

用独立浏览器 Profile/无痕上下文分别登录 Alice、IT、主管、设备人员。同一浏览器 context 跨 API 重启保留会话；不能在同一 context 的多个标签把不同用户登录混在一起。用户名来自初始化，不需要创建前端账号管理功能。

1. Alice 在“创建工单”填写标题、描述、网络类别和高优先级。创建成功为待处理/IT/v1，详情和基础列表来自 MySQL；先展示这个立即可见的事实，再解释 Search 异步延迟。
2. Alice 评论，版本到 v2。IT 认领到 v3、开始处理到 v4。每步详情和历史一起更新；操作带预期版本，并发修改需刷新后重新确认。
3. 主管跨团队转派到设备服务台，填写原因，版本到 v5。原 IT 打开详情被拒绝，新设备团队可见；Search 返回前也按当前 MySQL 权限复核，不依赖旧 ES 团队归属授权。
4. 设备认领/处理/评论/解决到 v9，Alice 确认关闭到 v10。展示两条评论、十条连续历史、首次解决时间及初始/当前团队区别。
5. Alice 再创建一张待处理工单，以便普通列表和搜索分页都有足够候选。加上十工单 Gold 共 12 张，不需要重新生成规模数据。
6. 主管打开 Search，关键词 DEMO、团队过滤、ID 升序、每页 10 条，展示下一页。保存首屏返回的 next_cursor 在浏览器或私有证据中，不投屏长串。
7. 主管看 Analytics，先用 2026-11-01 至 2026-11-04 的固定 Gold 窗口：新增 10、首次解决 6、平均 16 小时 50 分。当前积压不受日期窗口约束；本轮真实新工单会增加它。再切换为今天至明天，看真实业务新增/解决。

成功判据是浏览器可操作、MySQL 业务/历史吻合、ES/CK 传播后 Search/Analytics 返回正确版本和统计。刷新时间不等于追平证明；不可仅以页面出现、worker running 或 lag=0 宣布端到端一致。

## M4 技术演示

### A 服务时重建 B

在主管页面保持 A 的 Search/Analytics 可用，在另一终端执行：

~~~bash
python3 -B scripts/demo_steps.py rebuild -mode=create -job=demo_rebuild -target=both -batch-size=2 -organization=1 -start=2026-11-01 -end=2026-11-04
python3 -B scripts/demo_steps.py rebuild -mode=run -job=demo_rebuild -wait=60s
python3 -B scripts/demo_steps.py rebuild -mode=status -job=demo_rebuild
~~~

成功为 completed、total=applied、report consistent、difference_count=0，源/Topic/目标 UUID 和各分区 next_offset 完整。这里“冻结”是任务自己的快照和进度，不是长时间冻结普通业务；A API 保持运行。任务完成是对应时点的事实，不是推广许可。需要人工展示持续 follower 时可执行 follow，但最短主路径采用有限 run，再由交接 pin 做最终追平，不额外增加长期 follower 进程。

### 封存并交给 B 两个标准组

~~~bash
python3 -B scripts/demo_steps.py control -mode=freeze -reason="DEMO B handoff" > "$DEMO_RUNTIME/handoff-freeze.json"
python3 -B scripts/demo_steps.py control -mode=prepare -version=candidate_v1 -job=demo_rebuild -wait=60s
python3 -B scripts/demo_steps.py control -mode=handoff-begin -handoff=demo_handoff -version=candidate_v1 -wait=60s
python3 -B scripts/demo_steps.py control -mode=handoff-pin -handoff=demo_handoff -wait=60s
python3 -B scripts/demo_steps.py control -mode=handoff-init -handoff=demo_handoff -wait=60s
python3 -B scripts/demo_steps.py control -mode=handoff-consume -handoff=demo_handoff -wait=60s
python3 -B scripts/demo_steps.py workers-b
python3 -B scripts/demo_steps.py control -mode=handoff-verify -handoff=demo_handoff -wait=60s
python3 -B scripts/demo_steps.py control -mode=resume -freeze-id="$(jq -r .freeze_id "$DEMO_RUNTIME/handoff-freeze.json")"
~~~

pin 保存新的完整对账、相同源标记和实际 next_offset 向量，并封存 M4 写入权。init 只离线初始化本轮 B 两个新组，逐分区提交 **原 next_offset，不加一**，原始 FetchOffsets 读回确认。进入 CONSUMING 后只能从现有提交恢复，禁止重跑初始化；封存的 run/check/follow 不能再写 B。

`workers-b` 从持久 B 合同选择物理目标，并带准确 handoff 启动 consume/ck。它不能携带 A 的索引/空间配置；预演的首次错误接线已被已有身份检查拒绝，失败日志保留。verify 发布交接后的新标记，等待两组逐分区越过并完整对账，成功才显示 VERIFIED。此时 API 仍为 A1。

### 新证明、A→B 提交和 API 重启

~~~bash
python3 -B scripts/demo_steps.py control -mode=freeze -reason="DEMO promote A to B" > "$DEMO_RUNTIME/promote-freeze.json"
python3 -B scripts/demo_steps.py control -mode=read-begin -operation=demo_promote -direction=PROMOTE -version=candidate_v1 -read-revision=1
python3 -B scripts/demo_steps.py control -mode=read-switch-drain-plan -operation=demo_promote -api-scope="$DEMO_RUNTIME/scope.json"
python3 -B scripts/demo_steps.py control -mode=read-validate -operation=demo_promote -wait=60s
python3 -B scripts/demo_steps.py api-stop
python3 -B scripts/demo_steps.py control -mode=read-switch-drain-observe -operation=demo_promote
python3 -B scripts/demo_steps.py control -mode=read-switch-commit -operation=demo_promote -wait=60s
python3 -B scripts/demo_steps.py api-start --version B > "$DEMO_RUNTIME/b-api.json"
python3 -B scripts/demo_steps.py control -mode=read-switch-observe-ready -operation=demo_promote -api-boot="$(jq -r .boot_id "$DEMO_RUNTIME/b-api.json")" -probe-start=2026-11-01 -probe-end=2026-11-04 -wait=60s
python3 -B scripts/demo_steps.py control -mode=read-switch-resume -operation=demo_promote -freeze-id="$(jq -r .freeze_id "$DEMO_RUNTIME/promote-freeze.json")" -api-boot="$(jq -r .boot_id "$DEMO_RUNTIME/b-api.json")" -probe-start=2026-11-01 -probe-end=2026-11-04 -wait=60s
~~~

先在浏览器展示冻结写拒绝，再展示 PLANNED 的 Search/Analytics 503。关闭新读准入后仍必须等待旧 API 在途请求排空和 **PID 真实退出**；只发 SIGTERM、只看端口或 readyz 均不够。commit 重验当前冻结、精确新 marker、原始提交位置/保留范围、writer/heartbeat/member、UUID 和完整事实；一个 MySQL 事务切换两套读目标、递增 revision、保存 COMMITTED。

B 启动后业务仍冻结。observe-ready 必须验证当前 boot 真存活、readyz、真实 Search/Analytics/权限/统计以及 B 的状态范围。Resume 再验当前 boot，并在一个事务同时 OPEN 与 RESUMED。浏览器刷新后不用重新登录：Search 和 Analytics 的响应头均为 candidate_v1/revision 2，freshness/status 的 namespace 和消费组也必须是 B。

B 服务期间用 Alice 创建新工单并评论，然后确认 A/B 都包含它：

~~~bash
python3 -B scripts/demo_steps.py rebuild --version A -mode=check -target=both -organization=1 -start=2026-11-01 -end=2026-11-04 -wait=60s
python3 -B scripts/demo_steps.py rebuild --version B -mode=check -target=both -organization=1 -start=2026-11-01 -end=2026-11-04 -wait=60s
~~~

这两次 check 没有 job，直接对相应物理目标全量对账，不重启封存 follower、不改 Offset。两者 consistent/差异 0 后进入回滚；仍必须在新的冻结下再次证明 A。

### B→A 回滚

~~~bash
python3 -B scripts/demo_steps.py control -mode=freeze -reason="DEMO rollback B to A" > "$DEMO_RUNTIME/rollback-freeze.json"
python3 -B scripts/demo_steps.py control -mode=read-begin -operation=demo_rollback -direction=ROLLBACK -version=active_original -read-revision=2 -rollback-of=demo_promote
python3 -B scripts/demo_steps.py control -mode=read-switch-drain-plan -operation=demo_rollback -api-scope="$DEMO_RUNTIME/scope.json"
python3 -B scripts/demo_steps.py control -mode=read-validate -operation=demo_rollback -wait=60s
python3 -B scripts/demo_steps.py api-stop
python3 -B scripts/demo_steps.py control -mode=read-switch-drain-observe -operation=demo_rollback
python3 -B scripts/demo_steps.py control -mode=read-switch-commit -operation=demo_rollback -wait=60s
python3 -B scripts/demo_steps.py api-start --version A > "$DEMO_RUNTIME/a3-api.json"
python3 -B scripts/demo_steps.py control -mode=read-switch-observe-ready -operation=demo_rollback -api-boot="$(jq -r .boot_id "$DEMO_RUNTIME/a3-api.json")" -probe-start=2026-11-01 -probe-end=2026-11-04 -wait=60s
python3 -B scripts/demo_steps.py control -mode=read-switch-resume -operation=demo_rollback -freeze-id="$(jq -r .freeze_id "$DEMO_RUNTIME/rollback-freeze.json")" -api-boot="$(jq -r .boot_id "$DEMO_RUNTIME/a3-api.json")" -probe-start=2026-11-01 -probe-end=2026-11-04 -wait=60s
~~~

回滚复用相同状态阶段；A 的证明使用 A 的 legacy 组/投递/水位/心跳/真实进程，不能冒用 B 的 owner。要求本次 marker 全分区存在、committed 严格大于 marker offset 且在保留范围，完整 MySQL/A-ES/A-CK 对账一致。成功后选择 active_original/revision 3，Contract 指纹保持原值，B 和 CDC 继续运行。

浏览器再看 Search、Analytics 和旧团队拒绝，创建并评论新工单，检查两套继续追平。第一次 A 与 B 的有效游标在 A3 应为 `409 SEARCH_CURSOR_TARGET_CHANGED`，不会因回到 A 而复活。重新提交搜索表单从首屏开始，新游标分页正常；真正过期游标保留原 400 语义。

## 失败处理与用户可见行为

| 环节 | 成功标准 | 失败后的动作 |
| --- | --- | --- |
| 环境/初始化 | 每个服务归属、UUID、端口、数据目录正确 | 停止；不改用保留实例，不覆盖已有数据或标记 |
| 重建/候选准备 | 完整报告一致、位置完整、身份一致 | 保留失败任务和日志；只按原合同重试，身份/保留范围变化需新任务 |
| 交接初始化 | 每分区结果与原始读回都等于 pinned | 保持冻结；消费未开放时仅补齐同任务同位置；开放后禁止初始化 |
| PLANNED/新证明 | 同一冻结/修订/marker、两组和完整事实一致 | 保持当前读选择与冻结；查询持久状态，不能拿旧 VERIFIED 或 lag=0 放行 |
| API 排空 | 清单中全部实际进程退出、无未知/自动重启入口 | 禁止提交；保持冻结，不降低判据 |
| COMMITTED | 选择目标、revision+1 与操作审计吻合 | 禁止取消、手工回退 revision 或自动 Resume；恢复目的 API，必要时引用既有受控逆向恢复协议 |
| API_READY/Resume | 当前 boot 仍存活且真实探针再次通过 | API 死亡就重新启动并 observe；历史 ready 不可复用 |

~~~bash
python3 -B scripts/demo_steps.py control -mode=handoff-status -handoff=demo_handoff
python3 -B scripts/demo_steps.py status -operation=demo_promote
python3 -B scripts/demo_steps.py status -operation=demo_rollback
python3 -B scripts/demo_steps.py api-status
~~~

同一 operation 的 Commit/Resume 幂等；结果未知先读 MySQL，不重发不同参数或创建并发意图。普通 resume 不能越过未完成切换。PLANNED 尚未提交时，可显式 `read-cancel`，再用当前原 API boot 的 `read-switch-cancel-resume` 核验原目标健康后恢复；COMMITTED 后不能走此路径。复杂 SIGKILL 与逆向恢复参考 [C3 再验收](m4-03c3-revalidation.md)，本轮没有重跑其完整故障矩阵。

维护冻结只拒绝业务变更，详情/基础列表仍可读取。PLANNED 拒绝新投影读；排空期间 API 不接受连接，浏览器可能报连接错误。新 API 就绪后可查询，但恢复前业务写仍拒绝。不要求零停机；演示时必须口头说明维护窗口。

本轮已复现三个小体验问题，保留原行为：

- Search 409 提示正确，但“重试搜索”保留旧 cursor，继续 409。**操作方法：点击搜索表单的“搜索”或“清除条件”重开首屏**；建议后续按钮对该错误主动清 cursor/page。
- Analytics 在维护和连接失败时还声称“工单操作不受影响／可以继续使用”，冻结时不成立；读取连接失败还显示“提交结果待确认”。建议后续按维护/只读网络错误提供中性提示，不增加新状态框架。
- 登录页显示静态公共演示密码，私有随机密码与提示不一致。不要按照该提示登录新夹具；建议后续移除密码明文提示，由操作者私下提供凭据。

本轮没有出现无限加载、跨重启必须重新登录、权限回退或规范统计差异。上述结论限本轮记录的真实路径。

## 面试时展示哪些证据

终端只投屏小摘要：读选择 A1→B2→A3、freeze 与操作阶段、不同的新 marker、每分区 marker/committed/保留范围、A/B 对账 tickets/histories/difference_count，以及当前 API boot/读版本/状态 namespace。不要用 Offset 总和代表连续水位，也不要用 CK 物理行数比较代替规范事实。

最有说服力的顺序是：MySQL 同事务版本与历史 → 当前权限立即生效 → Kafka 双独立投影持续传播 → 重建候选完整对账 → 新冻结下证明、排空、原子选择 → 回滚新 revision 防止旧游标复活。解释两套投影用于可验证恢复和读目标替换；MySQL、CDC、ES、CK、API 以及同机磁盘/网络仍是单点，不表述为生产高可用。

本轮必要证据索引在 [demo-report.md](../artifacts/demo-01/20261116T102333.326700Z/demo-report.md) 与 [step-matrix.json](../artifacts/demo-01/20261116T102333.326700Z/step-matrix.json)。普通手工演示无需再做历史 artifacts 全量哈希、十万工单实验或全套 M1–M5 回归。只对本轮新变更和新资源做相应验证，历史失败与成功证据继续保留。
