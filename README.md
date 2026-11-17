# TicketPulse

TicketPulse 是一个 Go 工单系统，包含内嵌 Web 登录、创建与处理、评论、团队转派、关键词搜索和运营看板。MySQL 保存业务状态、权限、会话及操作历史；Kafka 传递已提交变更，Elasticsearch 提供搜索候选，ClickHouse 提供规范去重后的统计。

当前版本完成 M1–M5 的既定验收。M4 为 **CLOSED WITH LIMITATIONS**：支持独立投影重建、准确消费接续、完整对账，以及业务冻结下的受控 A→B→A 读切换。DEMO-01 已完成真实浏览器往返预演，DEMO-02 修复旧搜索游标重试、统计错误提示和登录密码提示。单机实验不等于生产高可用。

## 读代码与构建

| 路径 | 职责 |
| --- | --- |
| cmd/ticketpulse、web | HTTP API 与 Go 内嵌 HTML/CSS/JavaScript，产品运行不需要 Node |
| internal/ticket、internal/mysqlstore、internal/httpapi | 业务规则、MySQL 事务、当前权限、会话与 API 合同 |
| internal/search、internal/analytics | ES 候选与 MySQL 复核、CK 统计查询 |
| internal/syncpipe | Binlog 捕获、Kafka 发布、独立 ES/CK 消费、快照重建及对账 |
| internal/projection、internal/readruntime | 持久目标、冻结、读选择与同机进程排空观察 |
| cmd/ticketpulse-sync、cmd/ticketpulse-rebuild、cmd/ticketpulse-control | 同步、重建和操作者维护 CLI |
| db、configs | 新库结构、增量迁移、公开合成种子、无私有凭据的配置示例 |
| scripts、tests | 本机隔离夹具、定向回归和真实浏览器验收 |

依赖由 go.mod/go.sum 和 package-lock.json 固定。Go 模块要求 1.24+；本机已验证 Go 1.27.1、Python 3.9.6。原生服务与受控读切换脚本面向 macOS arm64。Node 20+、Chrome 和 Playwright 1.65.0 仅供浏览器测试。

~~~bash
# 项目根目录；这些命令不连接数据库或服务。
go mod download
go mod verify
make test
make vet
go build ./cmd/... ./internal/... ./db ./web ./scripts/demo_launcher

# 只有当前 bin/ 没有被运行进程使用时，才生成常规二进制。
make build
~~~

API、同步、重建和控制二进制分别是 bin/ticketpulse、bin/ticketpulse-sync、bin/ticketpulse-rebuild、bin/ticketpulse-control。运行中的受控进程使用固定可执行文件及哈希，禁止原地覆盖；隔离验收会编译到自己的 .local/ 时间戳目录。

当前源码包范围在 Makefile 中列明；保留目录的 artifacts 可能含不完整历史 Go 快照，所以同机不要使用仓根的 `go test/build/vet ./...` 扫描证据。只含跟踪源码的干净 checkout 不受这些快照影响。

## 最小独立启动与验收

新操作者先使用独立夹具，避免 make setup/run 等旧便利命令连接同一 checkout 的保留演示实例。下面的私有夹具从当前源码编译 API 与同步器，建立随机 schema、Topic、ES 索引和 CK 空间，加载十工单 Gold；不初始化 M4 读控制，也不修改历史实例。

| 组件 | 验证版本 | 最小夹具回环端口 |
| --- | --- | --- |
| MySQL | 8.4.5，ROW/FULL Binlog | 33317，独立 socket |
| Kafka／Java | 4.4.0／Temurin 21.0.13+9，单节点 KRaft、三分区、复制 1/最小 ISR 1 | 19112／controller 19113 |
| Elasticsearch | 8.17.5 | HTTP 19211／transport 19313 |
| ClickHouse | 26.10.2.103 | HTTP 18134／native 19012 |
| API/Web | 当前 Go 源码 | 18102 |

这些端口是该夹具的明确默认值，不是外部部署约定。未知占用、标记已存在、资源身份或容量不匹配时脚本拒绝操作。使用另一 checkout 或一个新的 marker 文件；不要删除旧标记或改用保留源。

~~~bash
# 仅准备版本固定且校验摘要的发行包，不启动/停止演示服务。
python3 -B scripts/demo_environment.py packages

# 示例标记可更名；必须位于 .local/demo-02/，且事先不存在。
export DEMO02_MARKER=.local/demo-02/first-verification.json
python3 -B tests/browser/demo02_fixture.py new
python3 -B tests/browser/demo02_fixture.py start
python3 -B tests/browser/demo02_fixture.py status
curl --fail --silent http://127.0.0.1:18102/readyz
~~~

打开 http://127.0.0.1:18102。用户名为 alice、it-agent、device-agent、supervisor、admin 等；随机密码由夹具生成，只保存在 marker 所指 runtime/environment.json 中，权限为 0600，由操作者私下读取。不要投屏、输出文件、提交它或把密码写入前端。db/seed.sql、db/login_seed.sql 和旧回归脚本包含公开合成测试凭据，只供本地回归；不能把默认种子当作生产账户初始化。该私有夹具会覆盖新库的测试密码，保留实例的密码不受影响。

浏览器定向验收：

~~~bash
npm ci
npx playwright test -c tests/browser/demo02.config.js

# 只定向停止 marker 所属的新进程，核对实际退出；保留全部数据及日志。
python3 -B tests/browser/demo02_fixture.py stop
~~~

status 验证源、服务进程和资源归属；readyz=200 证明当前 API 就绪。普通功能成功还要求真实登录、创建与详情查询、传播后的 Search，以及主管 Analytics 正确。测试结果和截图写入 marker 所指 artifacts/demo-02/<run>/；维护响应、断网和游标输入注入均明确标注，不冒充真实 M4 切换。

## 初始化与迁移

新库先加载 db/schema.sql → db/seed.sql → db/login_seed.sql，再按能力安装迁移。schema.sql 已含当前 M1 结构，新库不重复执行 001–005；已有库不得重导 schema/seed，应按各迁移前提检查后增量安装。

| 能力 | 顺序与入口 |
| --- | --- |
| Search／Analytics／Kafka | 006_search_sync.sql → 007_analytics_sync.sql → 008_kafka_sync.sql |
| 重建与完整对账 | ticketpulse-rebuild -mode=migrate：安装 **009 + 010**；local_rebuild.py migrate 仍仅做 009 |
| 持久目标及冻结 | ticketpulse-control -mode=migrate：011 |
| 候选消费交接 | control -mode=migrate-handoff：012 |
| 持久 API 读选择 | control -mode=migrate-read-control：013，安装不等于启用 |
| API 排空与就绪记录 | control -mode=migrate-read-runtime：014 |
| 回滚及 legacy writer 观察 | control -mode=migrate-read-rollback：015，在需要回滚证明的 A 消费者启动前安装 |

上述 CLI 的 DSN 必须指向已确认归属的源，控制连接使用 TICKETPULSE_CONTROL_DSN。Schema 安装、目标登记、读选择初始化是不同步骤；不因迁移完成而自动选择候选。M4 完整初始化及往返命令使用已有 [产品演示手册](docs/demo-runbook.md)，其中 demo_steps.py install 顺序调用 009–015 的官方接口。

旧 make setup、kafka-setup、analytics-setup 等脚本固定使用 .local 中的本机演示标记和 18080 这一组端口，只能由操作者在确认归属后使用。它们不是 Release 验收入口，不要在已有保留环境中照抄 setup/stop/reset 命令。旧 NATS 到 Kafka 的迁移仍见 [M2-02 交付](docs/m2-02-delivery.md)，不能重置 checkpoint 或从日志头替代协议。

## 外部已准备环境

[configs/sync.env.example](configs/sync.env.example) 是无私有值的连接示例。先在 .local/ 中复制一份并设置 0600，配置属于自己的 MySQL、Kafka、ES、CK 及稳定游标密钥，然后在各终端加载同一份文件。API 和同步器没有自动读取 .env 的逻辑，环境变量必须显式加载。端口由部署者决定，API 监听由 -listen 设置。

核心参数是 TICKETPULSE_DSN、TICKETPULSE_SYNC_DSN、TICKETPULSE_CDC_ADDR/USER/PASSWORD、TICKETPULSE_KAFKA_BROKERS、TICKETPULSE_ES_URL/SEARCH_INDEX、TICKETPULSE_CK_URL/CK_DATABASE。控制 CLI 另用 CONTROL_DSN。不要在命令行、版本库或日志中填写真实凭据；API、同步、复制与控制账号的权限应按职责配置。

服务启动顺序：MySQL/Kafka/ES/CK 就绪 → 所需迁移 → capture、consume、ck → API。一个业务源只启动一个 CDC；ES 和 CK 用独立组。新库首次捕获会执行既有快照初始化，不能对已登记托管源重新 bootstrap。

~~~bash
# 各自终端，先加载私有环境文件；只操作已确认的独立资源。
bin/ticketpulse-sync -mode=capture -server-id=12001
bin/ticketpulse-sync -mode=consume
bin/ticketpulse-sync -mode=ck
bin/ticketpulse -listen="$TICKETPULSE_HTTP_LISTEN" -routing=configs/local.json

# 只读观察，不以 lag=0 代替完整证明。
bin/ticketpulse-sync -mode=status
bin/ticketpulse-sync -mode=analytics-status
~~~

只启用 MySQL 时可以启动 M1；省略 ES 或 CK 端点分别关闭 Search 或 Analytics，页面及 API 以现有错误合同处理。启用 013 后不能省略 managed 目标预期或用环境变量覆盖权威选择。Docker compose.yaml 仅提供新 MySQL 的替代入口，依赖 TP_MYSQL_PASSWORD/ROOT_PASSWORD 和独立项目卷；不提供完整四组件部署，Docker/Linux 路径未经过本轮真实验收。

## 产品与技术演示

Alice 创建网络工单、评论；IT 工程师认领、开始处理、解决，Alice 关闭或重开。主管可带原因转派，旧团队立即失去 MySQL 详情访问权；Search 返回前也复核当前授权与业务版本。新文档未进入 ES 时暂不在候选集中，搜索是最终一致性，详情和基础列表使用 MySQL。

主管或管理员访问 Analytics，展示日期/团队筛选、新增、首次解决、平均首次解决时长和当前积压。日期使用 Asia/Shanghai、左闭右开；历史按事件团队，积压按当前团队且不受日期窗口约束。十工单 Gold 窗口 2026-11-01 至 2026-11-04 的新增为 10、首次解决为 6、平均为 60600 秒；额外实时工单会影响当前积压。

M4 使用同一业务源的两套物理投影。重建 B 在独立位置快照、接续 Kafka 并对账；候选封存后交给两个版本化组。A→B 和 B→A 每次都重新冻结、发布精确新标记、读回各分区提交位置并完整对账，排空全部受控旧 API，再原子更新读选择、受控重启、就绪验收和显式恢复写入。原 ACTIVE/PREPARED 登记不被读切换改写，read_revision 单调递增，旧游标不会因回滚复活。

手动主路径、维护窗口及停止条件只维护在 [demo-runbook.md](docs/demo-runbook.md)。完整恢复证据见 [M4-03B](docs/m4-03b-delivery.md)、[C1](docs/m4-03c1-delivery.md)、[C2](docs/m4-03c2-delivery.md)、[C3](docs/m4-03c3-delivery.md)。DEMO-02 改动和定向浏览器证据见 [交付报告](docs/demo-02-delivery.md)。

## 已验证范围与限制

- MySQL 业务状态、版本、评论和历史同事务；当前授权、创建幂等意图、Cookie/CSRF 有业务与浏览器证据。
- ES 严格 external version，CK 按业务键规范去重；成功或持久隔离后保存进度再提交 Kafka Offset。失败和位置可观察、可按现有协议恢复。
- M4-02 验证同机三 Broker 进程故障及复制确认边界；不代表跨故障域、网络分区或生产 SLA。
- 读切换限定 macOS 同机、可枚举且无未知自动重启的受控进程，需要维护窗口；没有热切换、自动回滚或跨主机 fencing。候选/legacy writer 都不能宣称生产级写入隔离。
- MySQL、单机控制与当前基础部署仍有单点；没有账号管理、密码重置、登录限流、自动退休投影或完整通用部署平台。首次发行包下载与 Docker/Linux 复现有明确未验收边界。

## 版本与证据

本地源码基线包含业务、测试、SQL、前端、脚本和说明。artifacts/（包括 Go 包内生成目录）、.local/、bin/、node_modules/、浏览器输出及凭据不跟踪，但保留在原位置。历史报告链接的原始证据需要同机 artifacts；纯源码 checkout 不包含这些大型实验资产。

~~~bash
git log -1 --format=fuller
git status --short
git show --stat HEAD
~~~

[API 合同](docs/api.md)、[事务设计](docs/design.md)、[实现与依赖说明](docs/reuse.md) 和 [Release-01 交付](docs/release-01-delivery.md) 用于学习与评审。其他报告记录当时的源码、运行环境和限制，不作为当前部署配置或在线服务承诺。没有远程仓库、推送或生产部署。
