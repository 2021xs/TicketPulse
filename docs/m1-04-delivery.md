# TicketPulse M1-04 Web MVP 交付

TicketPulse 现在可在浏览器完成账号登录、创建、团队受理、认领、处理、评论、转派、解决、重开和关闭。访问 **http://127.0.0.1:18080**，使用 alice、it-agent、device-agent、supervisor 或 admin；本地演示密码为 **TicketPulse!2026**。本轮沿用 feat/m1-01-ticket-loop，未暂存、提交或推送。

本文以 CODE 标识当前源码确认，RUNTIME 标识真实执行，INFER 标识基于验收的判断，UNKNOWN 标识未验证边界。开发前保存 53 个源码/配置/文档文件，集合 SHA-256 为 `3a63f94e710c027ef59c97dc1b7b57b5f45cd438f67c4045bb0d598b4011b9e9`，与 M1-03 最终源码无差异。[基线清单](../artifacts/m1-04/baseline-source-manifest.json) 和归档保留原内容，[完整 Diff](../artifacts/m1-04/changes.patch) 区分本轮改动。

CODE：Web 包含登录页、个人工作台三个 MySQL 列表、创建页、详情和全部 M1 动作。状态/优先级统一显示，评论与历史分区，详情展示创建人、负责人、当前/初始团队、生命周期时间、重开次数、解决说明及历史原因。页面支持分页、空结果、加载、失败重试、返回原列表上下文和基本响应式布局。

前端采用同源 HTML、CSS 和 ES 模块，由 Go embed 随二进制提供，无前端开发服务器或运行期 npm 依赖。规模适合当前几页 MVP，浏览器原生 fetch/表单即可接通已有接口；状态和权限规则留在原 Service/Store。页面根据服务端 options 展示动作，写接口仍逐次核验权限、源状态和版本。

CODE：账号与会话独立保存。密码是 bcrypt cost 12，登录不接收客户端用户/角色，停用账号统一拒绝。成功生成 32 字节随机凭证，MySQL 只保存 SHA-256，浏览器只通过 HttpOnly、SameSite=Strict Cookie 发送。会话绝对有效期默认 8 小时，读取不续期；再次登录轮换旧 Cookie，退出设 revoked_at。有效用户/角色/团队仍从当前数据库读取，原 Bearer API 保留。

Cookie 写请求同时检查同源 Origin 和与会话秘密绑定的 HMAC CSRF；登录也检查 Origin，防止跨站登录。没有 CORS 放行。HTTPS 或 secure-cookies 参数启用 Secure，本机 HTTP 回环 Cookie 不启用 Secure。页面转义所有动态文本，CSP 只允许同源脚本/样式；XSS 合成输入在实际 Chrome 中仅显示为文本。

| 新接口 | 输入与输出 |
| --- | --- |
| POST /api/v1/auth/login | username、password；200 返回 user、csrf_token、expires_at，并设置 Cookie |
| POST /api/v1/auth/logout | Cookie + Origin + X-CSRF-Token；撤销会话、清除 Cookie |
| GET /api/v1/me | 当前有效用户、有效成员团队、固定类别/优先级、Cookie 会话到期及 CSRF |
| GET /api/v1/tickets/{id}/options | 授权工单的展示动作、有效目标人员/团队及当前版本 |

详情新增 participants/teams，只解析该工单当前和历史引用人员/团队；列表新增当前团队名称。候选先检查详情权限，再按当前角色、源状态和有效成员关系筛选，不向无权用户提供人员或团队。详细合同在 [API 文档](api.md)，实际调用顺序在 [事务设计](design.md)。

CODE：Web → HTTP Cookie/Bearer 身份解析 → 原 Service → Store → MySQL。登录进入新会话事务；业务动作继续使用原工单行锁、expected_version 条件更新与事务历史。当前用户和候选读取同一请求内快照。前端每次提交携带读取版本，冲突后提示刷新，评论和解决输入在失败/登录中断后按用户保留。同一创建意图保存原 request_id，双击只发送一次，结果未知时锁定原内容供安全重试，主动新建另一张才换标识。

| 主要修改 | 位置 |
| --- | --- |
| Web 页面与交互 | web/index.html、app.css、app.js、embed.go |
| 登录/会话/CSRF、候选 HTTP | internal/httpapi/session.go、options.go、api.go |
| Service 与数据合同 | internal/ticket/session.go、options.go、service.go、model.go、list.go、errors.go |
| 会话/候选持久化与显示信息 | internal/mysqlstore/session.go、options.go、detail.go、list.go |
| 新库/迁移/登录种子 | db/schema.sql、migrations/005_web_sessions.sql、login_seed.sql、embed.go |
| 启动与验收 | cmd/ticketpulse/main.go、Makefile、scripts/local_mysql.py、smoke_session.py、browser_fixture.py、browser_acceptance.py |
| 真实测试 | internal/mysqlstore/session_integration_test.go、internal/ticket/session_test.go、tests/browser/mvp.spec.js、playwright.config.js |

新增 Go 依赖为 golang.org/x/crypto v0.45.0 的 bcrypt，最低 Go 版本升为 1.24。@playwright/test v1.65.0 是开发验证依赖，不参与产品运行。按含注释/空行的物理行统计，Go 业务净增 693、Go 测试 340、SQL 60、Python 工具 259、Web 资源 255、浏览器测试 433；文档、锁文件与构建配置另计，最终统计在 [代码量](../artifacts/m1-04/code-size.json)。

RUNTIME：005 已应用到本地演示库，新增 login_accounts 和 user_sessions，不改写原七张表。迁移前后逐列比对，原 5 张工单、29 条历史、2 条评论和 5 条幂等记录保持。重复 make migrate/setup 不覆盖已有密码或数据，部分 DDL 状态明确拒绝。新库初始化共用登录种子，MySQL 集成测试也复现旧结构后执行真实迁移。见 [迁移结果](../artifacts/m1-04/migration-result.json)。

RUNTIME：最新源码运行 make test、integration、vet、build 全部通过。integration 使用 MySQL 8.4.5，包含 -race -tags=integration -count=1；MySQL 56 个顶层测试、全包 67 个通过。六个本轮数据库测试覆盖密码、CSRF、Cookie 属性、固定到期、停用账号、退出/轮换、候选权限、迁移保留和失败不发 Cookie。M1-01–03 并发、历史回滚和业务权限全部回归。见 [执行摘要](../artifacts/m1-04/checks-result.json)、[完整日志](../artifacts/m1-04/final-integration.log)。

RUNTIME：make browser 用 Playwright 驱动本机 Chrome，四组全部通过，无跳过或 flaky，最新一次约 18.4 秒。每组在项目拥有的独立随机 MySQL 测试库上执行，检查实际 SQL 版本/状态/历史/评论，完成后清理。浏览器合成工单编号属于该随机库，与旧演示工单相同编号不表示覆盖。

| 浏览器场景 | 已验证结果与证据 |
| --- | --- |
| A 普通工单 | Alice 登录创建→IT 队列认领/开始→评论/解决→Alice 关闭；v6、六条连续历史、一条评论。[结果](../artifacts/m1-04/browser-scenario-a.json) |
| B 跨团队 | 主管改派→旧团队失权→设备队列认领/处理/解决→主管留原因重开→再次解决/人工关闭；v11，首次解决时间不变。[结果](../artifacts/m1-04/browser-scenario-b.json) |
| C 异常权限 | 错误/停用登录、退出后旧凭证拒绝、过期、CSRF、伪造身份、同创建超时重试、XSS、真实评论失败保留、旧版本冲突、停用负责人重新入队、失效团队主管恢复；恢复工单 v13，重开两次。[结果](../artifacts/m1-04/browser-scenario-c.json) |
| D 完整交互 | 同团队转派后旧负责人无解决按钮，新负责人可解决；双击创建一次、解决失败保留、真实列表分页/加载/错误重试；v6。[结果](../artifacts/m1-04/browser-scenario-d.json) |

[Chrome 总结果](../artifacts/m1-04/browser-test-result.json) 与 [测试库清理](../artifacts/m1-04/browser-cleanup.json) 可独立核对。[登录截图](../artifacts/m1-04/login.png)、[工作台截图](../artifacts/m1-04/workspace.png)、[跨团队详情](../artifacts/m1-04/cross-team-closed.png) 保存实际界面；无密码输入、Cookie 或会话凭证。

RUNTIME：本机 make setup 和原四个业务 smoke、新 smoke-session 均通过，业务行数保持 5/29/2/5。[HTTP 结果](../artifacts/m1-04/http-result.json) 明确区分 HTTP 与浏览器验证。用同一 Cookie 重启实际服务后，用户、团队和到期时间保持，随后退出失效，见 [重启会话](../artifacts/m1-04/restart-session-result.json)。最终服务在 127.0.0.1:18080 运行。

在项目目录执行 make setup、make run 后打开 Web，演示账号及多角色流程在 [README](../README.md)。浏览器验收另执行 npm ci、make browser；运行产品不需要 npm。所有变更基于本轮归档，[工作区结果](../artifacts/m1-04/workspace-result.json) 记录未暂存/提交/推送和原仓状态。

INFER：M1 的本地功能验收达到 PRD 要求，三类角色能在 Web 完成完整业务生命周期，登录/权限/版本/幂等/历史及主要异常交互已有 RUNTIME 证据。M1-03 明确的负责人失效重新入队和主管恢复例外已落实。M2/M3 的检索、统计与同步属于后续阶段。

UNKNOWN：未验证 Docker/其他平台、Safari/Firefox、连续运行 8 小时、Commit 网络中断、断电恢复或压力故障；已验证的是固定到期逻辑、1 秒真实到期、Chrome、本机 MySQL及实际服务重启。当前详情整条历史/评论一次读取，无长时间线分页；没有会话后台清理、登录限流、注册或账号配置界面。后二者不属于本轮 M1 合同，后续部署增强需另定范围。
