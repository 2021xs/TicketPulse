# TicketPulse M1 Web 与业务 API 合同

接口返回权威 MySQL 数据。Web 使用账号密码登录后的 Cookie 会话；原 `Authorization: Bearer <本地预置 token>` 路径继续支持回归。服务端从数据库取得有效账号、角色、组织与团队关系，请求中的身份字段/角色头部没有授权作用，创建 JSON 不允许 creator_id。有 Authorization 时只解析该头，不能用无效 Bearer 回退 Cookie 身份。

## 登录、退出与当前用户（M1-04）

`POST /api/v1/auth/login`，Content-Type 为 application/json，JSON 为 `{"username":"alice","password":"TicketPulse!2026"}`。账号规范化为小写并去掉两端空白，长度 1–64，允许字母/数字/_ . -；密码按原字节验证，长度 1–72 字节，数据库存 bcrypt cost 12 哈希。不存在、错误密码、停用账号统一返回 401 INVALID_CREDENTIALS，不泄露具体账号存在性。

登录要求 Origin 与实际请求 scheme/host 完全一致，拒绝 Sec-Fetch-Site=cross-site；JSON 只允许账号和密码。成功返回 200、user、csrf_token、expires_at，并设置 ticketpulse_session Cookie：安全随机 32 字节、43 位 base64url；HttpOnly、SameSite=Strict、Path=/、绝对有效期 8 小时。数据库只存其 SHA-256。原 Cookie 在再次登录时轮换并撤销；不同浏览器各自登录的会话独立。HTTP 本地回环未启用 Secure，HTTPS 自动启用，亦可用 -secure-cookies 要求；不使用不可信 X-Forwarded-* 推断 Origin。

`GET /api/v1/me` 返回 `{"user":{...},"teams":[...],"categories":[...],"priorities":[...],"csrf_token":"...","expires_at":"..."}`。user/teams 为当前数据库身份和有效成员团队。Cookie 登录返回绑定本会话的 CSRF 和到期时间；Bearer 请求的 csrf_token 为空、expires_at 为 null。没有密码、凭证原文或 token hash。刷新页面调用此接口恢复有效登录，读取不延长会话。

Cookie 的所有写请求，包括退出、创建和动作，必须同时满足同源 Origin 及 `X-CSRF-Token` 与本会话绑定值匹配；失败 403 CSRF_REJECTED。CSRF 值由会话秘密计算的 HMAC 提供，只从同源登录/me JSON 返回，页面内存保存；凭证由 HttpOnly Cookie 自动发送。没有 CORS 放行。Bearer 仍只凭显式 Authorization 验证，不要求 Cookie CSRF。

`POST /api/v1/auth/logout` 在数据库设置 revoked_at 后清除 Cookie，返回 `{"logged_out":true}`。旧凭证的新请求不可继续访问；有效期届满返回 401 SESSION_EXPIRED，已撤销/未知/停用身份返回 401 UNAUTHENTICATED。停用账号在每次会话读取及原业务事务有效用户检查中拒绝。已经完成请求身份检查的在途请求遵循原事务语义，退出不会撤销已提交业务操作。

session-ttl 默认 8h，允许 1s–24h；有效期从密码和当前身份校验后开始，fixed expiry 不滑动续期。会话在 MySQL 持久化，服务重启无需重新登录；当前没有会话后台清理或账号管理界面。

## 业务动作及候选数据（M1-04）

`GET /api/v1/tickets/{id}/options` 无额外查询参数。先按详情权限核验当前有效用户和工单，再返回 `{"actions":[...],"assignees":[...],"teams":[...],"current_team_active":true,"assignee_valid":true,"version":1}`。全部来自同一只读 MySQL 快照。

actions 为当前用户/状态可展示的原动作名称：claim、start、resolve、reopen、close、transfer、transfer-team、management-reopen、manual-close、comments。非负责人不展示处理/解决，管理角色无默认处理绕过权。团队失效的创建人仍可看到重开入口并得到明确恢复错误；所有写接口重新执行原行锁、当前权限、源状态及 expected_version 检查，候选不构成写授权。

assignees 只为有同团队转派权者返回当前有效团队的有效 Agent，排除已有负责人；无负责人时主管可选择有效成员。teams 只为有相应转派/恢复能力的管理角色返回组织内有效团队。普通提单人及失权成员不能通过此接口读取人员/团队候选。目标已失效或工单版本变化仍由实际动作接口拒绝。

## 创建工单

`POST /api/v1/tickets`，Content-Type 为 application/json，请求必须是一个 JSON 对象。

| 输入字段 | 本轮规则 |
| --- | --- |
| `request_id` | 必填，1–64 个 ASCII 字母、数字或 `_ . : -`；大小写敏感；UUID 字符串可用 |
| `title` | 必填，5–120 个 Unicode 字符，不能全部为空白 |
| `description` | 必填，1–5,000 个 Unicode 字符，不能全部为空白 |
| `category` | `network` 网络、`account` 账号、`device` 设备、`software` 软件、`other` 其他 |
| `priority` | `urgent` 紧急、`high` 高、`medium` 中、`low` 低 |

字符上限按照 PRD `4.2 的建议值在本轮冻结为 API 合同，后续 UX 调整须同步更新校验、DDL 与测试。请求体上限为 64 KiB，允许最长描述使用 JSON Unicode 转义。业务字符串保留原文；前后空白不被静默裁剪。

成功结果是 `{"ticket": {...}, "replayed": false}`，首次提交返回 HTTP 201，幂等重试返回 HTTP 200、replayed=true。响应 Location 指向详情路径。

| 工单字段 | 语义 |
| --- | --- |
| `id`、`ticket_no` | 服务端分配；编号形式为 TP-加至少 12 位十进制序号，允许间隙 |
| `organization_id`、`creator_id` | 从有效登录账号读取 |
| `title`、`description`、`category`、`priority` | 提交的创建内容 |
| `status` | 创建为 `pending`（待处理） |
| `assignee_id` | 创建为 null |
| `initial_team_id`、`current_team_id` | 创建时均为选中的有效受理团队 |
| `version` | 创建为 1 |
| `created_at`、`updated_at` | 创建时相同；RFC3339 UTC，微秒精度 |
| `first_resolved_at`、`closed_at` | 创建为 null |
| `reopen_count` | 创建为 0 |
| `resolution_note` | 创建为 null；后续保存最近一次解决说明 |

认领保持 `pending`（待处理），开始处理变为 `in_progress`（处理中）。解决进入 `awaiting_confirmation`（待确认），常规重开回到处理中，失效负责人重开重新入队为待处理；确认关闭进入 `closed`（已关闭）。

## 工单详情

`GET /api/v1/tickets/{id}`，id 为正整数。

返回 `{"ticket": {...}, "current_team": {...}, "assignee": null, "history": [...], "comments": [...], "participants": [...], "teams": [...]}`。工单字段同创建结果；current_team 返回 id、organization_id、name、active；有负责人时 assignee 返回账号 id、organization_id、name、role、active。无评论时返回空数组。participants 和 teams 只解析该授权工单当前/历史所引用的人员及团队，供创建人、评论作者、操作者和原/新团队名称显示；不返回无关成员或凭据。

详情权限依据 PRD `3.5：有效提单人查看本人创建的工单；有效 Agent 还需是当前有效团队的有效成员；Supervisor/Admin 查看其组织内工单。当前团队变更后，不再使用初始团队作为普通成员的授权依据。其他组织中的相同 ID 返回找不到工单，不返回对象字段。同组织无权访问返回明确拒绝且只含错误信息。

历史按 version 升序、id 升序排列。每条记录返回 id、ticket_id、action、operator_id、version、before、after、responsible_team_id、occurred_at，以及可空 reason、comment_id。创建记录 action=created、version=1、before=null；after 是创建完成时的业务状态。跨团队变更的 responsible_team_id 是变更生效后的目标团队，before/after 分别保存原团队和新团队；旧历史仍保留原归属。时间统一为 UTC，界面展示时可转换成 Asia/Shanghai。

## 认领与开始处理（M1-02A）

| 接口 | JSON 输入 | 成功响应 |
| --- | --- | --- |
| `POST /api/v1/tickets/claim` | ticket_id、expected_version，均为正整数 | 200，`{"ticket": {...}}` |
| `POST /api/v1/tickets/start` | ticket_id、expected_version，均为正整数 | 200，`{"ticket": {...}}` |

身份由 Cookie 会话或 Bearer 解析；请求中额外的 user_id、role、team_id、assignee_id 等字段一律拒绝。每次动作重新检查当前有效账号、Agent 角色、当前团队有效性和成员有效性，主管／管理员没有默认绕过权。

认领只允许待处理、无负责人、期望版本匹配的工单。成功把 assignee_id 设为当前 Agent，status 保持 pending，version 加一，updated_at 更新，并追加 claimed 历史。开始处理只允许仍在当前团队中的有效负责人，状态必须为 pending、版本匹配；成功改变 status 为 in_progress，保留负责人，version 加一并追加 processing_started 历史。

权限先于业务冲突核验。认领权限通过后先检查版本，再检查状态和负责人；两个工程师同时使用版本 1 认领，只有一人返回 200/version=2，另一人返回 409/VERSION_CONFLICT。重新读取版本 2 后再次认领，返回 ALREADY_CLAIMED。开始处理先检查当前负责人，再检查版本与状态，非负责人不会获得操作成功或版本信息。

动作没有 request_id 返回原结果的合同。一次成功动作即便丢失响应，沿用旧 expected_version 重试也不会再次变更；当前仍有权限时返回 VERSION_CONFLICT，转派后已失权的调用者按权限拒绝。客户端读取详情确认当前负责人、状态与历史。不能自动换成新版本并继续发送同一操作，重新执行应是读取后明确的新操作意图。

## 解决、重开与确认关闭（M1-02B）

| 接口 | JSON 输入 | 成功行为 |
| --- | --- | --- |
| `POST /api/v1/tickets/resolve` | ticket_id、expected_version、resolution_note | 200，处理中变待确认、版本加一 |
| `POST /api/v1/tickets/reopen` | ticket_id、expected_version | 200，待确认变处理中或异常重新入队、版本和重开次数加一 |
| `POST /api/v1/tickets/close` | ticket_id、expected_version | 200，待确认变已关闭、设置关闭时间、版本加一 |

三个接口的成功响应均为 `{"ticket": {...}}`，字段与详情中的当前工单一致。ticket_id、expected_version 必须为正整数；解决说明为 1–5,000 个 Unicode 字符，不能全部为空白，保留原文、换行和前后空格。解决接口示例：

~~~json
{"ticket_id":30,"expected_version":3,"resolution_note":"已修复主线路，连通性检查通过。"}
~~~

解决只允许当前有效 Agent 负责人，且仍是当前有效团队的有效成员。Supervisor/Admin 不绕过负责人检查。解决状态必须为处理中；首次成功把 first_resolved_at 设置为本次操作时间，再次解决保留首次值，并更新 resolution_note。详情返回最近一次说明；每次历史的 before/after 保留相应说明，因此再次解决不会覆盖旧历史。

重开和常规关闭只允许工单创建人。任何有效角色创建的工单，都按 creator_id 关系核验提单人身份；管理例外使用下述独立接口。重开只接受待确认：当前团队有效、原负责人仍为有效成员时保留负责人并回到处理中；当前团队有效、负责人停用/角色变化/成员失效/为空时清空负责人、回到待处理，历史动作为 reopened_requeued 并保存系统原因。两种成功分支均增加 reopen_count、version，保留首次解决时间及最近说明。当前团队失效则返回 409 TEAM_UNAVAILABLE，不变更任何数据，提示主管恢复到有效团队。确认关闭仍只要求有效提单人、待确认与版本匹配，不要求旧负责人或团队有效。

关闭设置 closed_at 为操作时间，首次解决时间、次数及解决说明保留。已关闭工单拒绝认领、开始、解决、重开和再次关闭等常规状态动作。版本冲突、源状态检查和权限拒绝沿用 M1-02A；权限先检查，授权通过后先核验 expected_version，再检查源状态。成功响应丢失后沿用旧版本重试返回 VERSION_CONFLICT，不会增加逻辑变更或历史。

解决说明通过迁移 003 保存；迁移 004 增加评论及历史原因，不改写旧历史 JSON。服务以每次请求的数据库身份为准。下面接口允许的 team_id、assignee_id 仅表示受校验的转派目标，不表示调用者身份。

## 转派与管理例外（M1-03）

全部接口为 POST，输入包含正整数 ticket_id、expected_version，成功返回 200 和 `{"ticket": {...}}`。

| 接口 | 额外输入 | 权限、源状态与成功行为 |
| --- | --- | --- |
| `/api/v1/tickets/transfer` | assignee_id；reason 可省略 | 当前有效负责人或组织 Supervisor/Admin；待处理/处理中；转给当前团队其他有效 Agent，保持状态 |
| `/api/v1/tickets/transfer-team` | team_id、必填 reason | 组织 Supervisor/Admin；待处理/处理中；改为不同有效团队，清空负责人并变待处理，保留 initial_team |
| `/api/v1/tickets/management-reopen` | team_id 可省略；必填 reason | 组织 Supervisor/Admin；仅待确认；按以下恢复分支重开 |
| `/api/v1/tickets/manual-close` | 必填 reason | 组织 Supervisor/Admin；仅待确认；设为已关闭并写 closed_at，无时长阈值 |

原因限制为 1–1,000 个 Unicode 字符、不能全部为空白，保留原文；同团队转派省略原因或传空字符串表示无原因，提供空白文本则拒绝。目标人员必须有效、角色为 Agent、属于当前有效团队；目标团队必须在当前组织且有效。当前负责人不能“转派给自己”，返回 ALREADY_ASSIGNED。未认领的待处理工单只允许主管/管理员指定负责人，普通成员不能以团队身份代替当前负责人。

管理重开整合人工例外和失效团队恢复：不传 team_id 或传 0 表示当前团队；当前团队有效时按负责人有效性返回处理中或待处理。如果指定不同有效团队，同一个事务完成重开、换团队、清空负责人、重新入队。当前团队失效且未指定有效新团队返回 TEAM_UNAVAILABLE；无效目标返回 INVALID_TEAM。每次成功重开只增加一次版本和重开次数，保存 management_reopened 历史及原因。待确认工单仍不能直接使用普通转派接口，已关闭工单不能通过管理重开恢复。

同团队转派历史为 assignee_transferred，跨团队为 team_transferred，人工关闭为 manually_closed。历史含操作者、原因、前后负责人/团队/状态。主管和管理员没有默认处理或解决权，需把工单交给有效 Agent。旧负责人在同团队转派后失去处理/解决权，仍可凭团队成员关系查看和评论；跨团队后旧团队普通成员失去详情、评论和团队列表权限，创建人及组织主管权限保留。

## 评论（M1-03）

`POST /api/v1/tickets/comments`，JSON 为 ticket_id、expected_version、content。content 为 1–5,000 个 Unicode 字符，不能全部为空白，保存原文、换行和前后空格。成功返回 200、`{"ticket": {...}, "comment": {...}}`。

评论对象含 id、ticket_id、author_id、content、version、created_at。作者由服务端身份取得。有效创建人、当前有效团队 Agent、组织 Supervisor/Admin 可评论；未获详情权限者不能读写评论。所有状态都允许授权评论，包括已关闭；评论保持工单状态、负责人、首次解决时间、关闭时间及重开次数。

一次成功评论在同一个事务插入评论、递增 ticket.version 和 updated_at、写一条 commented 历史，历史 comment_id 指向对应评论。正文由评论表保存，历史快照表示评论发生时的工单状态，不复制正文。详情在同一快照按 created_at ASC、id ASC 返回评论。无修改/删除评论接口、私密备注、附件或富文本。

评论也沿用旧版本冲突合同：响应丢失后保留原 expected_version 并回读详情，不能自动换新版本再次提交同一内容。M2 可以从工单版本/历史及评论表的已提交变更识别此次业务操作，本轮没有 CDC 或投影实现。

## 基础列表（M1-03）

`GET /api/v1/tickets?view=created_by_me&page=1&page_size=20`。

| 参数 | 合同 |
| --- | --- |
| view | 默认 created_by_me；允许 created_by_me、assigned_to_me、team_pending |
| page | 默认 1；范围 1–10000 |
| page_size | 默认 20；范围 1–100 |

只允许这三个单值参数；未知参数、重复参数、空值、非法整数均返回 INVALID_ARGUMENT。不能用 team_id、user_id、role 改变授权范围。

created_by_me 只查询当前有效用户创建的工单。assigned_to_me 查询本人仍为当前负责人且当前可见的工单；Agent 成员失效后不因旧 assignee_id 暴露数量，创建人关系及管理组织权限仍按详情合同生效。team_pending 只取待处理和有效当前团队：Agent 查询其全部有效团队成员关系，Supervisor/Admin 查询组织内有效团队队列，Requester 没有处理团队队列、返回空结果。认领后的待处理工单仍在队列中；开始处理后移出。使用 current_team，转派提交后的新请求立即反映新队列归属。

返回 `{"items": [...], "view": "...", "page": 1, "page_size": 20, "total": 0, "has_more": false}`。每行包含 id、ticket_no、title、priority、status、assignee_id、assignee_name、current_team_id、current_team_name、version、updated_at；无负责人时 ID/名称为 null。total 仅统计当前授权过滤后的范围，与分页数据在同一个只读快照查询。空结果返回 items=[]。

排序为 updated_at DESC、id DESC，LIMIT/OFFSET 分页。固定数据集保证同时间稳定排序、不重复不遗漏；数据在不同请求之间持续变更时不提供跨页快照合同。

## 错误

错误体为 `{"error": {"code": "...", "message": "..."}}`，不携带工单标题、描述、内部 SQL 或数据库凭据。所有业务响应带 Cache-Control: no-store。

| HTTP | code | 含义与客户端行为 |
| --- | --- | --- |
| 400 | `INVALID_ARGUMENT` | 必填、长度、枚举、JSON 或 ID 错误；修正输入 |
| 401 | `UNAUTHENTICATED` | 缺失凭据、凭据无效或账号停用 |
| 401 | `INVALID_CREDENTIALS` | 账号/密码错误或停用，重新输入 |
| 401 | `SESSION_EXPIRED` | 会话到期，重新登录 |
| 403 | `FORBIDDEN` | 当前身份没有查看权限 |
| 403 | `ACTION_FORBIDDEN` | 角色、团队成员或当前负责人不满足操作权限 |
| 403 | `CSRF_REJECTED` | Cookie 写请求的 Origin 或 CSRF 校验失败 |
| 404 | `TICKET_NOT_FOUND` | 当前组织不存在该工单 |
| 409 | `IDEMPOTENCY_CONFLICT` | 同用户 request_id 已对应不同创建内容；新创建意图生成新 ID |
| 409 | `VERSION_CONFLICT` | expected_version 不匹配；读取详情，不覆盖最新状态 |
| 409 | `ALREADY_CLAIMED` | 匹配当前版本，但工单已有负责人 |
| 409 | `INVALID_STATE` | 匹配当前版本，但源状态不允许该动作 |
| 409 | `TEAM_UNAVAILABLE` | 当前团队失效，主管需恢复到有效团队；待确认使用管理重开 |
| 409 | `ALREADY_ASSIGNED` | 目标已经是当前负责人 |
| 413 | `REQUEST_TOO_LARGE` | JSON 请求体超过 64 KiB |
| 415 | `UNSUPPORTED_MEDIA_TYPE` | 未使用 application/json |
| 422 | `NO_ACTIVE_TEAM` | 类别路由与默认团队都无效；修复配置后沿用原 ID 重试 |
| 422 | `INVALID_ASSIGNEE` | 目标人员不是当前团队有效 Agent |
| 422 | `INVALID_TEAM` | 目标团队不属于组织、不存在或已失效 |
| 503 | `CREATE_RESULT_UNKNOWN` | Commit 未得到可靠成功确认；保留原 ID 和内容重试 |
| 503 | `ACTION_RESULT_UNKNOWN` | 动作 Commit 未得到可靠成功确认；读详情，沿用旧 expected_version 重试可能冲突 |
| 503 | `REQUEST_TIMEOUT` | 请求超时；创建沿用原 request_id，动作读详情并保留原 expected_version |
| 500 | `INTERNAL_ERROR` | 数据库或内部错误；不返回成功；创建沿用原 ID，动作保留原 expected_version |

提交成功响应丢失与 Commit 结果未知都不能通过生成新的 request_id 解决。客户端保留原逻辑创建身份，服务端用数据库唯一约束与已提交结果安全重放。

## 工单搜索（M2-02 Kafka 链路）

GET /api/v1/search/tickets 复用 Bearer/HttpOnly Cookie 的服务端身份，无请求角色/用户覆盖参数。搜索使用 ES 投影匹配，返回当前 MySQL 已授权且版本追平的列表字段；详情仍调用原 GET /tickets/{id}。

| 参数 | 默认与合同 |
| --- | --- |
| q | 可选，最多 200 字；标题/描述 standard 分词、多 token AND；不搜评论 |
| ticket_no | 可选，工单号精确匹配，最多 64 字节 |
| status / priority / category | 可重复参数，同字段 OR；值为原业务枚举 |
| creator_id / assignee_id / current_team_id | 可重复正整数；assignee_id=0 表示未分配；团队是当前归属 |
| created_from / created_to | 带时区 RFC3339 时间；from 包含、to 不含 |
| updated_from / updated_to | 同上；时间须在 ES date_nanos 的 1970–2262 范围内 |
| sort | updated_at（默认）、created_at、priority、id；priority 按紧急>高>中>低排序 |
| order | desc（默认）或 asc；同值用 id 同方向排序 |
| page_size | 20（默认），1–100；每个多值字段最多 20 个值 |
| cursor | 返回的加密 next_cursor；首次省略，后续保持查询条件；绑定身份/查询，1 小时有效 |

不同字段 AND，时间窗左闭右开。未知参数、重复单值参数、非法枚举/ID/窗口均 400，不解释客户端 role、team、user header。固定数据稳定分页；持续变动不承诺跨页快照。

~~~http
GET /api/v1/search/tickets?q=network&status=pending&status=in_progress&priority=high&current_team_id=10&sort=updated_at&order=desc&page_size=20
~~~

~~~json
{
  "items": [],
  "page_size": 20,
  "has_more": false,
  "total": null,
  "total_relation": "unavailable",
  "freshness": {
    "state": "running",
    "last_projected_event_at": "2026-11-08T03:00:00Z",
    "last_success_at": "2026-11-08T03:00:00.050Z",
    "caught_up": null
  },
  "refreshed_at": "2026-11-08T03:00:01Z",
  "consistency": "eventual_projection_current_mysql_authorization"
}
~~~

items 使用已有 TicketSummary：主键、工单号、标题、优先级、状态、负责人/名称、当前团队/名称、版本、更新时间。ES 只取 ID/版本/排序，不返回描述或原始命中总数；MySQL 批量过滤无权对象及版本落后的候选，再提供列表字段。空结果不会泄露原始命中数。扫描找到下一条有效授权结果才设置 has_more；有下一页时增加 next_cursor。每页最多扫描 2,000 个 ES 候选，耗尽预算返回错误而不伪装完整成功结果。

freshness 表示同步进程心跳、最近投影事件和处理成功时间；running 不是“已经追平”。存在未恢复隔离记录标为 degraded；caught_up=null 代表没有可靠确认全链路同步水位。页面刷新与投影时间分别显示。

GET /api/v1/search/options 返回当前身份可选的有效团队与 Agent，用于页面筛选。Supervisor/Admin 为组织范围；普通身份只返回自己当前有效成员团队，人员来自这些团队有效 Agent，未分配由页面用 assignee_id=0 表示。该候选只用于交互，不扩大搜索/详情权限。

| HTTP/code | 行为 |
| --- | --- |
| 400 INVALID_ARGUMENT | 修正参数或重新开始分页 |
| 401 UNAUTHENTICATED / SESSION_EXPIRED | 原身份/会话合同，重新登录 |
| 503 SEARCH_UNAVAILABLE | ES 未启用、不可用、分片失败或不完整响应；不返回成功列表 |
| 503 SEARCH_TIMEOUT | ES/请求超时，不把部分响应当完整结果 |
| 422 SEARCH_TOO_BROAD | 授权扫描预算不足，需缩小范围 |

开启搜索所需配置、CDC/Kafka/ES 数据流及隔离恢复命令见 [M2-02 交付](m2-02-delivery.md)。M1 只依赖 MySQL 的运行入口保留；搜索不可用时仍能操作基础列表、创建和详情。

## 运营分析（M3-01）

`GET /api/v1/analytics` 使用原 Bearer 或有限期 Cookie 身份。服务端重新读取当前 MySQL 有效账号、组织和角色，仅允许 Supervisor/Admin。普通用户添加 role 或身份 header 无法读取统计。团队候选由服务端生成，包含停用团队的历史，跨组织 team_id 拒绝。

| 参数 | 合同 |
| --- | --- |
| start | YYYY-MM-DD，默认北京时间今天之前六天 |
| end | YYYY-MM-DD，默认北京时间明天；右边界不含 |
| team_id | 可重复，多值 OR，不选代表当前组织全部团队；最多 20 个 |

窗口为 1–366 天。未知参数、重复单值、非法日期或团队返回 400。日期只影响新增与首次解决；当前积压始终取最新 pending/in_progress，按当前团队归属。无首次解决时平均时长为 null，数量零为 0，不能用零时长表示无样本。

~~~http
GET /api/v1/analytics?start=2026-11-01&end=2026-11-04&team_id=10&team_id=20
~~~

~~~json
{
  "start": "2026-11-01", "end": "2026-11-04", "timezone": "Asia/Shanghai",
  "overview": {"created": 10, "first_resolved": 6, "average_first_resolution_seconds": 60600, "current_backlog": 5},
  "daily": [
    {"date":"2026-11-01","created":4,"first_resolved":1,"average_first_resolution_seconds":7200},
    {"date":"2026-11-02","created":3,"first_resolved":2,"average_first_resolution_seconds":45000},
    {"date":"2026-11-03","created":3,"first_resolved":3,"average_first_resolution_seconds":88800}
  ],
  "teams": [], "available_teams": [],
  "sync": {"state":"running","caught_up":true,"bootstrap_complete":true,
    "source_watermark_at":"2026-11-08T06:00:00Z","confirmed_through":"2026-11-08T06:00:00Z",
    "last_successful_write_at":"2026-11-08T05:59:58Z","pending":0,"ack_pending":0,"retrying":0,"isolated":0,
    "topic":"ticketpulse.ticketpulse_local.tickets.v1","consumer_group":"ticketpulse.ticketpulse_local.tickets.v1.ck.v1",
    "partition_offsets":[{"partition":0,"committed_offset":33,"log_end_offset":33,"lag":0}]},
  "refreshed_at":"2026-11-08T06:00:01Z","data_state":"ready",
  "backlog_scope":"current_latest_state_independent_of_date_range"
}
~~~

示例将团队数组简化；实际 teams 返回名称、有效状态、指标及 backlog_drilldown_url，available_teams 返回整个当前组织候选。同步响应另含 applied、processing_attempts、heartbeat_at、recovered。每个配置分区都返回独立 Offset，不能跨分区比较大小。

ready 代表已确认当前观察点追平，empty 代表追平且组织无工单，partial 代表初始化、积压、失败或水位尚未确认。两张分析表允许短暂最终一致，隔离或积压不会标成完整数据。refreshed_at 是页面请求时间；confirmed_through 是所有分区都已提交源标记的最小时间。刷新不是追平证明。

| HTTP/code | 行为 |
| --- | --- |
| 400 INVALID_ARGUMENT | 日期、参数或组织团队无效 |
| 401 UNAUTHENTICATED / SESSION_EXPIRED | 账号停用或会话无效 |
| 403 ANALYTICS_FORBIDDEN | 当前角色无统计权限 |
| 503 ANALYTICS_UNAVAILABLE | CK 未配置、不可用或响应不完整 |
| 503 ANALYTICS_TIMEOUT | CK 超时，缩小窗口重试 |

只有当前积压返回 `/search?current_team_id=...&status=pending&status=in_progress`，不带历史日期。历史新增与首次解决没有精确 Search 下钻。搜索异步，未同步时明细可能暂少于 CK 当前积压，页面明确提示；追平样本核对四张正确工单。详情始终由当前 MySQL 提供。

## M4-03A 维护和目标版本

业务冻结后的创建、认领、处理、解决、关闭、重开、转派和评论返回 HTTP 503，业务错误为 BUSINESS_WRITES_FROZEN。详情、基础列表、登录和退出继续可用。维护操作只由持有独立 MySQL 控制权限的命令行执行，没有冻结、恢复或目标推广的 HTTP API。

新搜索游标绑定目标版本、当前用户、组织、查询条件和有效期。相同目标版本可正常续页；不同目标版本或升级前未携带目标字段的游标返回 HTTP 409 / SEARCH_CURSOR_TARGET_CHANGED，客户端必须重新搜索。授权、版本复核、排序、lookahead 和 2000 个候选上限保持原合同。修改加密密钥仍会使游标解密失败，目标变化的判定不依赖修改密钥。

GET /healthz 返回进程存活状态。GET /readyz 在普通模式检查 MySQL 连接，在受管理模式实时核对统一目标合同、源身份、Kafka Topic／分区、ES 索引 UUID、CK 两张表 UUID、消费进度及状态来源；失败返回 HTTP 503 和 state=not_ready。就绪响应不包含端点、业务数据或内部错误。冻结期间只读服务可以保持 ready；ready 不代表投影实时追平或消费位置已经交接。

受管理模式的开启、安装前提及本轮安全边界见 [M4-03A 交付](m4-03a-delivery.md)。

M4-03B 新增的交接只通过维护 CLI 操作候选。DRAINING、PINNED、OFFSETS_READY、CONSUMING 任一未完成阶段都会拒绝恢复业务写入；VERIFIED 后仍需显式提供 freeze_id 解除冻结。候选消费及状态读取按目标版本和新 Group ID 隔离，VERIFIED 不会修改 API 的 ACTIVE 目标、搜索游标或统计查询空间。维护命令和真实故障证据见 [M4-03B 交付](m4-03b-delivery.md)。
