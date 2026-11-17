# DEMO-02 演示体验问题修复

DEMO-02：**PASS**。三处前端问题已修复，独立真实服务上的 Chrome 定向验证最终 13 项通过。维护响应和断网由浏览器夹具注入，单独标注；本轮没有重新执行 A→B→A，也没有向保留服务发布改动。

源码基线为 `feat/m1-01-ticket-loop`，没有有效 HEAD，文件未跟踪。实施前记录的 200 个源码文件中，只有 `web/app.js` 内容变化（增加 20 行、删除 7 行）；新增三个定向测试文件及本报告。内容清单、原文件和可重建差异见本轮[证据索引](../artifacts/demo-02/20261116T131513.965539Z/evidence-index.json)。

## 三处根因与修改

| 问题 | 原因及修改后的行为 | 当前源码位置 |
|---|---|---|
| 旧搜索游标无法重试 | [CODE] 原错误按钮只重新渲染当前 URL，因此继续提交旧游标。现在仅对 `SEARCH_CURSOR_TARGET_CHANGED` 显示“重新搜索”；点击复制当前查询，删除 `cursor`、`search_after`、`page`，回到第一页。关键词、多选筛选、时间、团队、负责人、排序保留。没有自动重试，普通错误仍走原重试路径。 | [searchPage](../web/app.js#L132)，新增处理在 133–140 行；普通分页在 130–131 行。 |
| 统计失败文案误导 | [CODE] 网络异常没有区分 GET 与写请求；统计错误页还承诺业务可继续。现在 GET 断网显示“暂时无法连接服务，请稍后重试”，GET 超时显示“查询超时”；显式维护错误显示维护提示，其余统计不可用显示中性提示。失败页不再显示“提交结果待确认”或承诺业务可写。POST 结果未知提示保持原语义。 | [api](../web/app.js#L28)：41、47–48 行；[errorText](../web/app.js#L52)：53–54 行；[analyticsPage](../web/app.js#L186)：188–193 行。 |
| 登录页密码提示错误 | [CODE] 登录帮助硬编码了示例密码。改为“请使用演示负责人提供的账号和密码”，不包含具体凭据。账号校验、Cookie、CSRF 和会话实现未改。 | [loginPage](../web/app.js#L71)：74 行。 |

[CODE] 当前后端会将统计目标检查失败统一映射为 `ANALYTICS_UNAVAILABLE`（[service.go](../internal/analytics/service.go#L32)），其原始提示包含“工单操作不受影响”（[model.go](../internal/analytics/model.go#L171)）。前端用中性统计提示覆盖该文案，不能仅凭这个错误码判断是否冻结。明确维护错误码才显示“系统正在维护，统计数据暂时不可用，请稍后重试”。后端错误合同保持原样。

新增测试文件：[demo02.spec.js](../tests/browser/demo02.spec.js)（389 行，浏览器用例和脱敏请求证据）；[demo02.config.js](../tests/browser/demo02.config.js)（21 行，有界面 Chrome、单 worker、不自动重试）；[demo02_fixture.py](../tests/browser/demo02_fixture.py)（222 行，独立资源启动、身份校验和定向停止）。

## 真实浏览器验证

[RUNTIME] 2026-11-16 21:31:18–21:32:09（北京时间），macOS arm64，Chrome 149.0.7827.54，Playwright 1.65.0；最终 **13 PASS、0 FAIL、0 SKIP**，约 50.4 秒。实际命令：

```bash
npx playwright test -c tests/browser/demo02.config.js
```

API 使用私有 `127.0.0.1:18102`；MySQL 33317、Kafka 19112（控制端口 19113）、ES 19211、CK 18134。随机数据空间为 `ticketpulse_test_demo02_5312ed734f7bbbdc`，Topic、索引和 CK 空间均由该私有源派生。源和进程身份见 [environment.json](../artifacts/demo-02/20261116T131513.965539Z/environment.json)。只安装新库基线与 006/007/008，没有安装或调用 M4 读切换控制。

| 验证 | 结果与证据类型 |
|---|---|
| 搜索首页、下一页 | PASS；真实 Go→ES→MySQL 查询，两页为 10/2 条。 |
| 409 后重新搜索 | PASS；夹具签发目标已变化的测试游标，真实未改动的后端返回 409。点击后请求不含旧游标，第一页 10 条，所有原查询条件及排序保持，随后下一页正常。不是本轮真实 M4 切换。 |
| `search_after` 防御性清理 | PASS；模拟 409 响应后清除三个分页参数，真实首屏查询成功。该参数不是公开 Go 搜索参数。 |
| 过期游标 | PASS；夹具签发已过期游标，真实后端返回 400 `INVALID_ARGUMENT`，仍为普通“重试搜索”，未误分类为目标变化。 |
| 正常统计与权限 | PASS；真实 Gold：新增 10、首次解决 6、平均 60600 秒；团队 10 为 7/4。普通用户请求返回 403。 |
| 维护、统计不可用 | PASS；明确标注的模拟 503 分别验证维护和中性文案，未承诺业务可写。解除注入后真实查询恢复 200。 |
| HTTP 查询失败 | PASS；真实非法日期 400 可修正重查；模拟 500、503 和非 JSON 502 均为读取失败提示。 |
| 断网、超时 | PASS；浏览器模拟离线，恢复后真实查询 200；私有延迟响应触发真实浏览器 12 秒超时。页面不再显示 GET 提交结果未知，也未持续转圈。服务未因这些故障停止。 |
| 登录与会话 | PASS；页面无静态密码，真实错误凭据 401、正确凭据 200；Cookie 为 HttpOnly、SameSite=Strict、Path=/，刷新不改变 Cookie/会话到期时间，退出后查询身份为 401。 |
| 写请求回归 | PASS；模拟 POST 断网仍保留结果未知提示；随后真实创建、评论、认领、处理、解决、关闭成功，工单 124 为 v6，6 条历史和 1 条评论。 |

原始 [浏览器报告](../artifacts/demo-02/20261116T131513.965539Z/browser/2026-11-16T13-31-18-506Z/report.json) 与 [执行日志](../artifacts/demo-02/20261116T131513.965539Z/browser-final-133117.log) 可核对每项结果。请求 JSON 和截图位于 [最终 results 目录](../artifacts/demo-02/20261116T131513.965539Z/browser/2026-11-16T13-31-19-340Z/results/)；配置被 Playwright 重载后，报告和实际截图目录时间戳不同，索引记录了真实路径。

已检查的用户可见截图：

- [旧游标 409](../artifacts/demo-02/20261116T131513.965539Z/browser/2026-11-16T13-31-19-340Z/results/demo02-Search-normal-pages-cd243-in-every-filter-and-sorting/real-409.png) 与 [重新搜索的第一页](../artifacts/demo-02/20261116T131513.965539Z/browser/2026-11-16T13-31-19-340Z/results/demo02-Search-normal-pages-cd243-in-every-filter-and-sorting/fresh-first-page.png)。
- [模拟维护提示](../artifacts/demo-02/20261116T131513.965539Z/browser/2026-11-16T13-31-19-340Z/results/demo02-Analytics-explicit--9dff5-te-copy-simulated-response-/simulated-maintenance.png) 与 [浏览器离线提示](../artifacts/demo-02/20261116T131513.965539Z/browser/2026-11-16T13-31-19-340Z/results/demo02-Analytics-browser-n-3b464-tral-copy-and-real-recovery/browser-offline.png)。
- [无密码提示的登录页](../artifacts/demo-02/20261116T131513.965539Z/browser/2026-11-16T13-31-19-340Z/results/demo02-Login-no-static-pas-96696-ixed-session-remain-correct/login-no-password.png)。密码输入区域已遮盖；请求证据不保存登录体、Cookie、CSRF 或游标密钥。

## 定向回归与失败记录

| 命令 | 结果 |
|---|---|
| `node --check web/app.js`，以及两个新增测试 JS 的语法检查 | PASS。 |
| `go test -race ./internal/search -run TestReadRevisionCursorCannotReviveOnSameContract -count=1 -v` | PASS，既有跨修订号游标合同。 |
| `go test ./internal/analytics ./internal/httpapi -count=1` | PASS，两个包的单元回归；不冒充数据库集成测试。 |
| Python 夹具 `ast.parse` | PASS。 |

[RUNTIME] 真实生命周期完成后，额外只读检查私有源：35 张工单、81 条历史、2 条评论。35 张工单的 ID/版本/状态在 MySQL、ES、CK 一致，ES 外部版本相同，CK 规范历史键数为 81；投递状态只有成功或旧版本跳过，三个同步 worker 均运行。详见 [源数据检查](../artifacts/demo-02/20261116T131513.965539Z/final-private-source.json) 和 [传播复核](../artifacts/demo-02/20261116T131513.965539Z/final-private-propagation.json)。这是字段及数量复核，未宣称完整内容 Reconcile。未重跑 M4 全套故障和十万工单实验。

前两次失败尝试全部保留：第一次 11 项通过，暂停 Playwright 路由的超时注入被浏览器识别为网络错误，超时断言失败；第二次定向重跑重复创建搜索样本，数量断言失败。夹具改为真实延迟 HTTP 响应、持久复用样本，先定向 2 项通过，再完整 13 项通过。早先样本仍在私有库，没有清理。最终 35 张包含 Gold 10 张、两批搜索样本各 12 张及最后的生命周期工单。全部报告与读检查中的两次查询修正记录见 [failure-index.json](../artifacts/demo-02/20261116T131513.965539Z/failure-index.json)。

## 环境保留与结论边界

[RUNTIME] 私有 API、三个同步进程、MySQL/Kafka/ES/CK 均核验身份后定向 SIGTERM，实际退出已观察，8 个私有端口关闭；数据目录和所有日志保留，见 [停止记录](../artifacts/demo-02/20261116T131513.965539Z/stopped.json) 与 [收尾观察](../artifacts/demo-02/20261116T131513.965539Z/protection-closeout.json)。

保留服务 18080 在实施前后均返回 200；60555、18101 在实施前即无法连接，收尾结果相同。本轮未重启或修复它们。基线 30 个历史进程中，收尾 29 个身份相同，候选消费者 PID 19476 已退出；[UNKNOWN] 退出原因未核实，本轮没有向该进程发送信号，不能宣称全部历史进程始终存活。没有连接保留业务数据源执行迁移、写入、Offset 调整或 M4 控制操作。

三项修复没有新的任务阻塞。普通搜索、分页、权限、Gold 和登录会话有本轮定向证据，后端及控制协议内容哈希未变。[INFER] 这些证据支持演示体验修复完成；原有 M4 能力的完整验收仍引用 DEMO-01/M4 历史材料。改动仅在工作区和新私有测试二进制验证，未部署到保留服务，未暂存、提交或推送。
