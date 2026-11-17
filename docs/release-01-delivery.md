# Release-01 本地源码基线与复现验收

Release-01 的源码、构建和独立启动验收已完成，发布边界为 **RELEASE_READY_WITH_LIMITATIONS**。本地 Git 基线只纳入项目源码及说明，历史数据和证据留在原位置；没有远程仓库或生产部署。实际提交 ID、树 ID 与提交结果保存于本轮 [git-commit.json](../artifacts/release-01/20261116T153550.678272Z/git-commit.json)，最终含字面提交 ID 的 [release-report.md](../artifacts/release-01/20261116T153550.678272Z/release-report.md) 在提交后生成，避免报告引用自身提交造成循环。

## 源码与生成物

[CODE] 原分支为 feat/m1-01-ticket-loop，没有有效 HEAD 或 index。读取 Git refs、配置、工作区及 fsck 后确认这是预期的未出生分支，不是待覆盖的既有历史。逐项纳入 cmd、internal、web、db、tests、scripts、configs、docs，以及 AGENTS.md、README、Makefile、Go/npm 锁文件、Compose 和 Playwright 配置；准确清单、文件摘要和 index 模式见 [纳入清单](../artifacts/release-01/20261116T153550.678272Z/tracked-source-manifest.json)。

.gitignore 排除所有 artifacts 目录（含 Go 包内证据）、.local 私有实例、bin、node_modules、浏览器/Python 输出、日志、下载临时文件和私钥/本地凭据文件。未删除、移动、压缩改写任何历史资产。源码与测试中的公开合成种子仍保留，它们不是生产账户初始化；Release 私有夹具会生成随机密码，只写 0600 本地配置。

本轮只修改交付相关文件：README、.gitignore、Makefile、configs/sync.env.example、demo-runbook，以及 DEMO-02 夹具和两份测试的 marker 读取。业务 Go、前端业务修复、SQL 与 M4 合同未改。夹具新增 new 入口和私有 marker 选择，拒绝覆盖旧 marker 或写出 .local/demo-02；Makefile 固定真实源码包范围，避免仓根 ./... 把历史不完整 Go 快照编进来。修改前后内容与差异保存于本轮证据。

[CODE] README 现在区分最小普通功能验收与完整 M4 演示；009+010 必须使用官方 rebuild migrate，011–015 使用各显式 control 迁移。Schema 安装不等于启用读选择。外部连接模板不含私有地址或凭据，真实值只在操作者的私有文件中配置。旧 make setup/stop、历史 18080/60555/18101 不作为新版本启动承诺。

## 敏感信息检查

本轮对拟纳入文件作本地模式扫描、固定凭据定位、脚本 AST 核对及私有值精确匹配。67 个私有配置文档中的 16 个凭据值与源码比对，零命中；没有发现私钥正文、真实 Token/JWT、私有 DSN、Cookie 或游标密钥进入拟提交内容。固定本地测试账户、故障日志探针、公开摘要和连接占位符已区别检查，不把普通字段名当作泄漏。

准确结果见 [私有值比对](../artifacts/release-01/20261116T153550.678272Z/private-secret-exact-match.json) 与 [暂存检查](../artifacts/release-01/20261116T153550.678272Z/staged-audit.json)。这是本地源码审计，不宣称证明所有可能的秘密均不存在；不回显私有值，不上传原始数据。提交只使用明确文件路径，不使用 git add .、-A、force 或改写历史。

## 当前源码验证

[RUNTIME] 当前源码在全新私有实例上验证：MySQL 8.4.5、Kafka 4.4.0/Temurin 21、ES 8.17.5、CK 26.10.2.103；API 18102，其他端口和源 UUID/进程身份见 [独立环境索引](../artifacts/release-01/20261116T153550.678272Z/test-evidence-index.json)。源为 ticketpulse_test_demo02_f89f191ced2c1916，与所有保留环境分离。

| 验证 | 实际结果 |
| --- | --- |
| go mod verify | PASS，依赖锁文件未改。 |
| 真实源码范围 go build / make vet | PASS；包括 API、同步、重建、控制、db/web embed 和演示 launcher。 |
| 真实源码范围 go test -race | PASS，未设置 integration tags/保留 DSN；不冒充历史真实故障矩阵重跑。 |
| Python unittest | PASS，3 项；脚本 AST 和 JS 语法检查通过。 |
| 当前源码 Chrome 定向测试 | 13 PASS、0 FAIL/SKIP，2026-11-16 23:49:02–23:49:30 北京时间，约 27.5 秒。 |
| 登录/会话 | 正确登录 200、错误凭据 401，Cookie/刷新/退出正常，页面无固定密码提示。 |
| 创建与生命周期 | 浏览器真实创建、评论、认领、处理、解决、关闭；新工单 v6、历史连续、评论正确。 |
| Search/Analytics | 正常第一页/下一页、权限与团队过滤；Gold 10/6/60600 秒。旧游标 409 重新首屏成功，过期仍为 400。 |
| 传播补查 | 23 张工单、64 条历史、1 条评论；MySQL/ES/CK 的 ID/版本/状态一致，ES 外部版本匹配，CK 规范历史键 64。 |

维护、网络断连、HTTP 延迟与错误响应均为已标注的浏览器注入；游标使用私有夹具签发测试输入，由真实后端判断 409/400。未运行新的 A→B→A、Broker 故障矩阵或十万性能测试。小规模字段复核不冒充 M4 完整内容 Reconcile。

原始命令、日志、请求 JSON、截图和失败记录留在 [本轮证据目录](../artifacts/release-01/20261116T153550.678272Z/)。初次仓根 go build ./... 因历史快照缺 embed 文件失败，记录保留；修正为明确源码范围后通过。干净 index 检出/提交源码归档的构建验证另见最终报告，不以原工作区能编译代替纳入完整性证明。

## 历史进程核查

[RUNTIME] 18080 根页面返回 200，它是旧版本演示 API，healthz/readyz 为 404；不能用当前新增健康接口判断旧二进制异常。60555 是 C3 保留 A3 API（历史 PID 19672），18101 是 DEMO-01 保留 A3 API（PID 66130），两者当前已无监听。DEMO launcher 仍报告存储的子进程身份，但 ps 证明子进程已不存在；status 本身不是存活证明。

C3 候选 ES 消费者 PID 19476 已退出。历史原始日志记录 2026-11-16 21:22:09 的 CK 身份查询超时和 PROJECTION_TARGET_MISMATCH，当前 TargetConsumerConfig.Check 会检查两端合同并保守退出。它不是正常自动清理，也不能据此推断数据已丢；那套 B-ES 后续追平能力不能再用历史 VERIFIED 证明。该 C3 的继续观察还被原有 8 GiB 预算拒绝（目录约 10.01 GB），本轮没有放宽预算或清理。

[UNKNOWN] 两个历史 API 退出的原因与当前历史组完整追平未进一步证明。源码显示保留 live fixture 等显式停止，不自动重启；当前并不承诺历史实例一直在线。它们不是本次独立 Release 复现的前置服务。本轮保留所有旧进程、数据、Offset 和历史证据，未自动修复；详细观察见 [历史进程记录](../artifacts/release-01/20261116T153550.678272Z/retained-process-observation.json)。

## 可复现性与剩余边界

README 的最小私有启动从 new→start→status→真实浏览器使用→stop 已跑通。私有进程经身份核验后实际退出，数据和失败记录保留。没有基于保留源做迁移、冻结、OffsetCommit、重绑或重启。

[INFER] 当前版本足以用于源码学习、有限技术评审与新私有产品演示。版本完整性用精确纳入清单、暂存审计、干净检出构建和提交/归档事实支撑；历史大型实验仍作为既有证据。

[UNKNOWN] 本轮复现使用已有校验过的发行包，未重验空缓存外网下载，也未真实跑 Docker/Linux、跨主机 HA 或生产级 fencing。纯源码 Git checkout 不包含历史 artifacts，历史报告的原始日志链接需要同机保存的证据。M4 继续保持 CLOSED WITH LIMITATIONS；本轮没有新增业务逻辑、可靠性框架或中间件。
