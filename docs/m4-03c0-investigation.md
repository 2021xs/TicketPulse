# TicketPulse M4-03C0：API 读目标推广与安全回滚实施前核查

当前源码有受控切换所需的重建、消费接续、冻结和身份校验基础，但还不能直接完成安全的 API 读目标推广。最小缺口是：持久 API 读目标选择及启用修订号、与新冻结绑定的新鲜证明、受控 API 排空／重启记录、候选状态读器接线，以及阻止提前 Resume 的推广记录。直接修改现有 ACTIVE/PREPARED 会破坏 A/B 的校验合同。

本轮于 2026-11-15（Asia/Shanghai）读取 live repository 与已保存证据，只新增本报告。未连接原演示服务，未执行测试、迁移、冻结、推广、回滚、Offset 操作或服务启动；没有修改业务源码、配置和历史资产。源码定位以本次读取为准。

关键判断如下，详细依据分布在 A–H：

- [CODE] 详情直接读 MySQL，ES/CK 推广不改变详情和当前权限的来源。
- [CODE] API 的 ES/CK 客户端及版本在启动时固定；受管理 Search/Analytics 在查询开始校验 ACTIVE，但没有锁住该判断直到响应完成。
- [CODE] API 仍使用旧 freshness 和旧 CK 状态；直接换端点会产生“候选数据、旧组状态”的组合。
- [CODE] ACTIVE 校验只接受原组的首次登记，交接校验又要求候选保持 PREPARED；两者不能靠改一列兼容。
- [CODE] 已 VERIFIED 的交接再次 Verify 只查身份，既不发新标记，也不重新对账。
- [INFER] 最小方案应独立表达“当前 API 读目标”，同时保留 A/B 的不可变合同和写入归属；业务冻结、停止并排空 API、持久切换、受控重启和显式恢复构成维护窗口。
- [INFER] 回滚必须重新冻结、验证目的投影的新标记和完整内容，并增加读启用修订号；重置 Offset 或恢复旧报告都不能证明回滚安全。

| 术语 | 本报告中的含义 |
| --- | --- |
| 登记版本、ACTIVE/PREPARED | 011 中的现有登记角色及历史验证合同；当前 ACTIVE 还承担 API/legacy worker 的校验 |
| API 读目标选择器 | 建议新增的单例记录，原子选择同一版本的 ES、CK 及状态来源；本轮未实现 |
| 读启用修订号 | 建议每次推广或回滚都递增的读 revision，区别 A→B→A 中的两次 A |
| owner/epoch | 012 的目标写入进程归属；不是 API 读启用修订号，也不是后端请求级 fencing |
| 新鲜证明 | 新冻结下的新源标记、各分区实际提交、当前 writer 状态及完整对账组成的推广／回滚证据 |
| [CODE] / [RUNTIME] / [INFER] / [UNKNOWN] | 当前源码／历史真实运行／推论或建议／尚未核实的事实 |

## A. 当前代码与版本证据

[CODE] 分支为 `feat/m1-01-ticket-loop`，`git rev-parse --verify HEAD` 失败，没有有效 HEAD；`git ls-files` 为空，项目文件均未跟踪。没有用 git diff 为空判断改动。按 M4-03B 的最终源码清单逐文件检查：调查开始时的 262 个源码、配置与文档文件全部匹配，没有新增、删除或内容变化。比对基线是 [final-source-manifest.json](../artifacts/m4-03b/final-source-manifest.json)，记录时间为 2026-11-14T10:49:44.796516Z。本轮新增报告后为263个文件，业务源码仍为同一基线。

[RUNTIME] 重新读取 B 的原始日志确认21项交接专项 PASS、7项基础专项 PASS、MySQL78项与 syncpipe11项相关回归 PASS；子进程专用辅助测试只在父运行中 SKIP。四类运行的175个可执行源码文件都与 B 最终快照匹配。核对 [test-summary.json](../artifacts/m4-03b/test-summary.json)、[closeout.json](../artifacts/m4-03b/closeout.json)、[专项日志](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/integration.log) 及对应基础／回归日志；没有重新运行这些实验。B 的951个已登记证据文件也逐一复核哈希，零变更。

[CODE] 本轮收尾还按 [previous-evidence-manifest.json](../artifacts/m4-03b/previous-evidence-manifest.json) 重新核验5,499个更早历史资产的大小与SHA-256，全部一致；这只是本地文件完整性核验，不是新的业务或服务运行验收。

已安装迁移必须区分实例与时间，源码文件存在不能证明某个数据库已安装：

| 证据 | 能确认的迁移事实 | 本轮边界 |
| --- | --- | --- |
| [CODE] [db/embed.go](../db/embed.go#L35)、[EnsureHandoffSchema](../internal/projection/handoff.go#L145) | 011/012 可显式安装；012先执行011，再建七张交接／候选状态表 | API普通启动不自动迁移；本轮未执行该函数 |
| [RUNTIME] B夹具源码 [m403bNew](../internal/mysqlstore/m403b_handoff_integration_test.go#L91)、专项日志 | 在当次新建的随机库安装011/012后运行交接，实际成功 | 夹具结束清理各自随机库；不表示当前还有同名测试库 |
| [RUNTIME] [演示保留证据](../artifacts/m4-03b/preservation/1794653375905073000/demo.json) | 2026-11-14T10:49:35Z，演示库011四张表、012七张表均为0 | 这是历史元数据观察；本轮未连接原演示库，不声称今天的安装状态 |
| [RUNTIME] [B私有服务收尾](../artifacts/m4-03b/environment-closeout/20261114T104927.473458Z/stop.json) | B私有服务已经停止，数据目录保留 | 未为核对DDL而启动这些服务 |
| [UNKNOWN] 下一轮实际推广实例 | 仍需核对源UUID、schema、实际DDL、版本／交接行、进程及端点 | 只能在用户指定的独立实例执行身份核验 |

当前没有通用 migration-version 账本。012 在011的控制日志留下安装记录，缺表时由 [HandoffInstalled](../internal/projection/handoff.go#L168) 检查该记录和现存控制表，防止误退回 legacy。这可复用于新读控制元数据的保守拒绝，但不能替代实际DDL核验。

下列关键文件的完整SHA-256固定本次结论；其余文件可由B最终清单检验。

| 文件 | 本次内容SHA-256 |
| --- | --- |
| cmd/ticketpulse/main.go | 52b1c8ebc2b7c34c8f758b9098ff60523b1efd548e6ca8db5ec961a81379d26e |
| internal/projection/model.go | 27b880b4d7e2734f8c1fa757674a01ea2b5ed9427d251d793490e9d50054ed15 |
| internal/projection/store.go | b441b9b465c5cbfd07d70157ad7425997c936253155fe545803bd8edd079383b |
| internal/projection/gate.go | 09205f46a44f1a162eefc152ea4280defd66ab0dcb464f51721c3df4f49ac4a1 |
| internal/projection/handoff.go | 0b96579413d572953c2ffab60d63f81df052e942c6a9197234a17d959b8cb9f0 |
| internal/syncpipe/projection_control.go | 1e5f71d9d0eaf3a5966ab6021052caa5901c496908952216f82748e6b6a77f8a |
| internal/syncpipe/target_status.go | 4cd8645cbd35f247dcadb2682bd04cebca62b85eb55756d20589e0f51272794c |
| internal/search/service.go | 5eac334174b4174d31fa05ea9452b182b96d0c96f1c4dfdc64092d2bd5a42b99 |
| db/migrations/011_projection_control.sql | 8a5af4bb7c6e2f0dfb756c00ecaf9638f8f29f4fa3273e722c3ea2ba22bef728 |
| db/migrations/012_projection_handoff.sql | a2f3efbfba3e73fc6a5cb55204f327aa78f1979ea5223d968493f631ca7fcc2b |

## B. Search、Analytics及状态的实际读路径

### 普通详情与基础列表

[CODE] 最短详情链为 HTTP `GET /api/v1/tickets/{id}` → `API.detail` → `ticket.Service.Detail` → `mysqlstore.Store.Detail`。详情开启 MySQL RR只读事务，在同一快照中读取当前用户、工单、权限、团队、负责人、历史和评论，完成后提交读事务。没有 ES、CK、目标版本或业务闸门读取。基础列表也使用 MySQL RR只读事务。

| 入口／symbol | 位置 | 关键事实 |
| --- | --- | --- |
| HTTP注册、API.detail | [httpapi/api.go:75](../internal/httpapi/api.go#L75)、[145](../internal/httpapi/api.go#L145) | 鉴权、解析ID后调用业务详情，直接返回结果 |
| Service.Detail | [ticket/service.go:134](../internal/ticket/service.go#L134) | 校验用户与ID，再调用Repository |
| Store.Detail | [mysqlstore/detail.go:14](../internal/mysqlstore/detail.go#L14) | 第16行开RR只读事务，第25行读工单，第29行最终授权，第49／77行读历史／评论 |
| Store.List | [mysqlstore/list.go:13](../internal/mysqlstore/list.go#L13) | 当前MySQL数据及权限，不依赖投影身份 |

[INFER] 固定投影切换不需要把详情改成ES，也不需要为已知ID引入额外补偿。整个API受控停止期间详情会随该进程短暂不可用；冻结本身不阻止它读取。

### Search

[CODE] 最短链为 `GET /api/v1/search/tickets` → `API.searchTickets` → `search.Service.Query` → `Elasticsearch.Candidates` → `searchAccess.Authorized` → MySQL摘要及游标。ES端点、索引、AES-GCM游标密钥和Search对象在API启动时构造；运行中的进程不会重新读取环境变量。

| 环节／symbol | 位置 | 实际行为 |
| --- | --- | --- |
| API启动的ES选择 | [cmd/ticketpulse/main.go:91](../cmd/ticketpulse/main.go#L91) | 读取TICKETPULSE_ES_URL、TICKETPULSE_SEARCH_INDEX，构造固定客户端 |
| ES客户端 | [search/elasticsearch.go:31](../internal/search/elasticsearch.go#L31)、[36](../internal/search/elasticsearch.go#L36) | endpoint/index为对象字段，持有HTTP连接池；没有运行期目标更新器 |
| HTTP搜索 | [httpapi/search.go:112](../internal/httpapi/search.go#L112)、[170](../internal/httpapi/search.go#L170) | 参数与身份校验后调用Search服务 |
| 每次搜索的版本检查 | [search/service.go:123](../internal/search/service.go#L123) | 仅配置了check的受管理模式执行；失败映射SEARCH_UNAVAILABLE |
| 受管理接线 | [cmd/ticketpulse/main.go:114](../cmd/ticketpulse/main.go#L114)、[128](../cmd/ticketpulse/main.go#L128) | TICKETPULSE_TARGET_VERSION指定版本，传入managed.Check及CursorTarget |
| BuildQuery候选过滤与排序 | [search/elasticsearch.go:171](../internal/search/elasticsearch.go#L171)、[239](../internal/search/elasticsearch.go#L239) | 组织／身份／团队及业务条件筛选；主排序+ID平局键；普通搜索使用search_after |
| Elasticsearch.Candidates实际请求 | [search/elasticsearch.go:254](../internal/search/elasticsearch.go#L254)、[260](../internal/search/elasticsearch.go#L260) | 请求固定索引/_search；仅取id/version，不返回原始ES总数 |
| 当前MySQL权限 | [mysqlstore/search.go:32](../internal/mysqlstore/search.go#L32)、[85](../internal/mysqlstore/search.go#L85) | 请求内RR快照，批量查当前工单和可见团队；返回字段来自MySQL |
| 版本与安全预算 | [search/service.go:175](../internal/search/service.go#L175)、[220](../internal/search/service.go#L220) | 候选版本必须等于MySQL版本；2000候选上限，page_size+1 lookahead |
| 候选批次／推进 | [search/service.go:247](../internal/search/service.go#L247)、[261](../internal/search/service.go#L261) | 首批100，低选择性后续最多400；按候选尾排序键推进，返回游标绑定最后返回结果 |

[CODE] 普通Search不持有PIT，只用固定索引的search_after。完整对账另外使用 [OpenReconcileView](../internal/search/reconcile.go#L20) 的PIT，两条分页合同不能混淆。受管理的ES身份检查 [Identity](../internal/search/identity.go#L15) 要求具体索引UUID，alias不能被当作已验证的具体索引。本轮无需引入alias。

[CODE] Search的freshness当前来自 [mysqlstore.SearchFreshness:125](../internal/mysqlstore/search.go#L125)，读取 `sync_worker_state(es)`、旧 `sync_kafka_es_deliveries` 的隔离和成功时间；不带VersionID，新候选状态不在这些表中。API启动时第108行和受管理重建第128行都传入原store作为freshness读器。当前旧目标与该状态来源是一致接线；如果下一轮仅替换ES客户端，该接线就不能描述候选。

### 游标身份及回滚后的失效

[CODE] [cursor结构:65](../internal/search/service.go#L65) 保存用户、组织、查询指纹、到期时间、排序位置及Target。受管理Target来自 [Version.CursorTarget:43](../internal/projection/store.go#L43)，值为版本ID加不可变Contract摘要。签发期限为一小时（[Search.Query:235](../internal/search/service.go#L235)）。[decodeCursor:86](../internal/search/service.go#L86) 先核对解密、用户／查询／期限，再比较目标，差异返回 SEARCH_CURSOR_TARGET_CHANGED；HTTP映射在 [api.go:279](../internal/httpapi/api.go#L279)，为409。

[INFER] A→B切换后，仍在有效期且密钥／用户／查询一致的A游标会409。B→A回滚如果直接恢复同一 `A:ContractHash`，未到期的早先A游标可能再次被接受；当前只有目标指纹，没有“这一次启用A”的修订号。建议在游标Target外层增加稳定的读启用修订号，每次推广／回滚递增，Contract本身不改。心跳、owner epoch、阶段等动态值不能混入这个Target。

[CODE] “过期游标继续返回409”与当前合同不相符：真正超过Expires的游标先返回 INVALID_ARGUMENT/400；密钥变更导致无法解密也为400。409适用于仍可解密、身份与查询有效，但读目标或启用修订号已变的游标。建议保持已有过期语义，下一轮分别测试400与409，不能笼统宣称所有旧游标都409。

### Analytics与状态来源

[CODE] 最短链为 `GET /api/v1/analytics` → `API.analytics` → `analytics.Service.Query` → `StatusReader.AnalyticsStatus` → `ClickHouse.Metrics`。主管权限仍由当前MySQL校验，日期与业务统计口径保持M3。状态在统计数据查询之前读取，二者不是跨系统事务快照。

| 环节／symbol | 位置 | 实际来源 |
| --- | --- | --- |
| API启动的CK选择 | [cmd/ticketpulse/main.go:74](../cmd/ticketpulse/main.go#L74) | TICKETPULSE_CK_URL、TICKETPULSE_CK_DATABASE固定构造 |
| CK请求 | [analytics/clickhouse.go:26](../internal/analytics/clickhouse.go#L26)、[46](../internal/analytics/clickhouse.go#L46)、[Metrics:178](../internal/analytics/clickhouse.go#L178) | 客户端固定endpoint/database；查询参数使用该database，两张表在同一空间 |
| HTTP及当前主管授权 | [httpapi/analytics.go:52](../internal/httpapi/analytics.go#L52)、[mysqlstore/analytics.go:11](../internal/mysqlstore/analytics.go#L11) | 当前MySQL用户／团队；普通用户拒绝 |
| 查询检查、状态、统计 | [analytics/service.go:32](../internal/analytics/service.go#L32)、[38](../internal/analytics/service.go#L38)、[46](../internal/analytics/service.go#L46) | managed.Check在查询开始；之后先状态、再CK指标 |
| 当前API状态接线 | [cmd/ticketpulse/main.go:83](../cmd/ticketpulse/main.go#L83)、[89](../cmd/ticketpulse/main.go#L89) | ConnectStatusQueue没有Target，AnalysisStatus用旧组／旧表 |
| legacy CK状态 | [syncpipe/analytics_status.go:17](../internal/syncpipe/analytics_status.go#L17) | Queue.Group(ck)、旧bootstrap、旧heartbeat、水位、delivery |
| 候选状态分流 | [syncpipe/analytics_status.go:18](../internal/syncpipe/analytics_status.go#L18)、[TargetStatus:19](../internal/syncpipe/target_status.go#L19) | 只有Queue.Target不为空才使用候选状态；API目前没有设置它 |

[INFER] 对当前受管理旧API，错配配置通常会在启动／检查中失败，不能因此称已有Bug。但未管理模式允许单独配置ES/CK，且新候选API尚无正确接线。下一轮应从一个读目标记录同时解析客户端、freshness、统计状态和组归属。只复制旧CK caught_up，或只改CK database，都不足以推广。

### 启动、就绪及旧进程

[CODE] 启动在绑定HTTP监听之前调用 [ProjectionControl.Managed:302](../internal/syncpipe/projection_control.go#L302)。[ManagedProjection.Check:313](../internal/syncpipe/projection_control.go#L313) 每次重新查持久版本，要求仍为ACTIVE且Contract指纹与本进程一致；随后检查业务闸门结构有效、源／Topic／分区、ES索引UUID、CK两表UUID、旧组历史提交和全局进度绑定。检查期限5秒。FROZEN也是有效闸门，所以受管理只读请求可在冻结时继续。

[CODE] `/healthz` 仅存活，`/readyz` 执行配置的Readiness，失败503；普通模式只Ping MySQL。受管理模式把managed.Check同时用于readyz、Search查询和Analytics查询。它不是整个HTTP mux的统一准入：详情、列表、搜索选项和身份接口没有该检查（[httpapi/api.go:54](../internal/httpapi/api.go#L54)、[Options:15](../internal/httpapi/session.go#L15)）。

[INFER] 如果通过未来合法协议让已运行旧API的ACTIVE判定失效，它的新Search/Analytics请求可被拒绝；进程不会自动换客户端，也不会自动退出，healthz和MySQL只读路径仍可用。当前check只在请求开始执行，已通过检查的请求可以在切换后完成旧目标查询。不能把readyz失败等同于HTTP进程完全停止服务。

[CODE] API使用 [http.Server:134](../cmd/ticketpulse/main.go#L134)，SIGINT/SIGTERM触发 [Shutdown:142](../cmd/ticketpulse/main.go#L142)，等待期限5秒；没有API单例锁或持久启动ID记录。`-listen`可指定不同端口，所以单个端口的占用不能证明不存在第二个API。HTTP外层请求期限10秒，ES/CK客户端期限分别5／6秒；5秒Shutdown超时本身不能证明全部处理完成。

## C. API切换可能出现的混读窗口

[CODE] 业务闸门只排空业务写事务。创建与动作最先获取闸门FOR SHARE（[create.go:24](../internal/mysqlstore/create.go#L24)、[actions.go:73](../internal/mysqlstore/actions.go#L73)），冻结取得FOR UPDATE后确认；读路径不取得闸门。登录／退出及CDC控制操作也不属于工单业务冻结。HTTP统一设置Cache-Control:no-store（[api.go:96](../internal/httpapi/api.go#L96)）；当前查询模块没有投影结果缓存，但对象和连接池持有启动时目标。浏览器已展示的旧结果与已发送响应不能靠服务端切换撤回。

以下为[INFER]失败窗口，不是本轮重新注入的运行结果：

| 窗口 | 当前代码产生的风险 | 最小关闭条件 |
| --- | --- | --- |
| 只冻结，然后分别重启两个API | 读请求仍允许；两进程各持有自己的ES/CK配置 | 先统一维护入口／单机进程清单，停止并证明旧API退出，再提交读选择 |
| 先改ES配置，后改CK配置 | 一个新进程或多个进程可能读不同投影；legacy模式无法统一验证 | 一个单例选择器同时引用完整目标版本和状态范围；启动全部匹配才监听 |
| 查询目标换成候选，状态读器仍旧 | Search状态描述旧ES组；Analytics的partial/ready可能根据旧CK组计算 | 同一读版本解析数据客户端与状态读器 |
| 旧请求刚通过ACTIVE检查，控制器立即切换 | 请求继续固定旧目标，无跨请求生命周期的读锁 | 切换前受控排空；请求开始校验作为防误启动保护，不能代替排空 |
| 控制器提交后SIGKILL，旧API尚存活 | 没有可靠恢复阶段；旧进程／legacy模式仍可能响应 | 持久记录旧API退出证明、原／新版本及启用修订号；不证明退出则不提交 |
| 新API启动失败或启动配置指向旧版本 | 没有“已切换、未就绪”的持久区分；B的Resume仍可能允许 | 推广操作阶段与确切启动ID；未就绪禁止Resume |
| A→B→A恢复旧指纹 | 早先A的未到期游标可能恢复有效 | 每次切换都增长读启用修订号，回滚也不倒退 |
| 省略受管理参数启动旧API | 当前普通模式不查ACTIVE，能绕过新读选择保护 | 新控制已启用的源库，新版API不得省略受管理准入；未启用的演示模式保持兼容 |

建议的维护窗口顺序为：持久业务冻结与排空 → 登记待切换操作并阻止Resume → 停止并证明旧API及请求排空 → 新标记与目的投影完整证明 → 原子提交读选择 → 受控启动匹配新修订号的API → 就绪和两条真实读路径核验 → 显式恢复业务。不要求零停机。

[INFER] 为防止“已观察旧API退出，自动重启又进入”的竞态，待切换操作还应关闭受管理投影读准入及旧版本启动准入。PLANNED期间拒绝新的Search/Analytics和旧读版本重启；已进入的请求仍通过受控排空结束。COMMITTED之后仅允许匹配当前version/hash/read_revision的API启动，保持业务冻结以供真实读路径验收。新API的请求校验应查询读选择与操作阶段，不能只查询登记phase。

[CODE] 当前A/B版API只执行ManagedProjection.Check，不读取上述尚未新增的选择器。若保留登记ACTIVE，仍在运行的A/B二进制不会因新选择器提交而自动失效。[INFER] 因而下一轮必须先把受控API升级为识别读控制的版本，或证明所有旧二进制进程及其自动重启入口均已停用；仅写入新控制表不能约束旧二进制或任意手工启动的旁路进程。当前单机进程管理及自动重启配置未观察，属于[UNKNOWN]。

[INFER] 停止证明至少绑定同机host、PID及启动nonce、可执行版本、实际监听端口、进程退出观察和操作ID。对已登记的旧进程逐个观察退出／所有受控监听关闭；无法证明某个进程结束，保持冻结。不要仅依赖发出SIGTERM、过了5秒、某个named lock空闲或某个端口关闭。异常Shutdown后仍须确认进程已经退出。单机驱动可沿用已有隔离脚本的身份核验方式，无需新协调服务。

## D. 持久推广状态与SIGKILL恢复要求

### 现有ACTIVE不能直接变更

[CODE] [011:18](../db/migrations/011_projection_control.sql#L18) 的版本表用phase记录ACTIVE/PREPARED；第28行generated active_slot使ACTIVE唯一，第30行只允许这两种phase。两个进度绑定行以组件为主键，全局引用当时ACTIVE版本。MySQL可以在一个事务中先解除旧active_slot，再建立新active_slot并更新相关行，但当前没有这样的方法，也没有完整的推广校验与恢复记录。

[CODE] 代码限制比DDL更严格：[Version.Validate:139](../internal/projection/model.go#L139) 的ACTIVE分支要求Candidate/NextWriter为空，writer为原topic.es.v1/ck.v1；[PREPARED分支:143](../internal/projection/model.go#L143) 要求M4 Candidate、rebuild_job写入归属及旧原组NextWriter提示。[Handoff.Validate:66](../internal/projection/handoff.go#L66) 又要求候选保持PREPARED且Contract摘要不变。API检查的原始条件为：

```go
// internal/syncpipe/projection_control.go:327
if v.Phase != projection.Active || v.Fingerprint() != m.Version.Fingerprint() {
    return fmt.Errorf("configured version is not the pinned ACTIVE contract")
}
```

[CODE] VersionStore.Get内部调用Validate；Insert只支持首次登记／准备，不提供更新接口（[store.go:47](../internal/projection/store.go#L47)、[68](../internal/projection/store.go#L68)）。原ACTIVE改PREPARED因没有Candidate而无效；候选改ACTIVE因Candidate、M4 writer及NextWriter字段而无效。即使绕过这些验证，候选运行中的Handoff校验仍失败，API的Managed.Check又会把整个Contract与只包含legacy组的live contract比较（[projection_control.go:327](../internal/syncpipe/projection_control.go#L327)）。不能提供一段裸SQL作为下一轮安全方案。

[CODE] Fingerprint只hash Contract，phase、checked_at、updated_at及handoff状态不参与。修改phase理论上不改指纹，修改writer／Candidate等Contract内容则会改变指纹并破坏已有交接摘要。API启用修订号应独立存放。

### 两条最小路线的比较

下面为[INFER]方案比较；两条都需要新的切换操作记录，现有A/B表无法表达API重启与读选择提交。

| 路线 | 优点 | 必须处理的成本／边界 | 建议 |
| --- | --- | --- | --- |
| 新增API读目标选择器，保留登记phase | 保留B的PREPARED／VERIFIED、owner和原组合同；legacy writer/CDC的检查可继续使用原登记合同 | API全部改用选择器作为唯一“读ACTIVE”；文档必须区分登记ACTIVE和API当前读版本，不允许两个查询真相并存 | 推荐首版，符合只切读目标的范围 |
| 真正更新现有phase和全局进度绑定 | 外部查询版本表可直接看到新的ACTIVE | Version.Validate需按合同类型而非角色验证；Handoff需允许phase变化；API、legacy writer、CDC的ACTIVE依赖要分开；更新进度绑定需识别候选状态范围 | 若必须让版本表phase表示当前读ACTIVE，应明确选择此路线并完整回归，不能只改DDL |

推荐路线不是把两份ACTIVE同时供API选择。新单例是启用功能后的唯一读选择，原phase保留A/B登记兼容职责；新API不再把旧generated active_slot视为查询源。`sync_projection_progress_bindings`也保留旧worker的登记绑定，不拿它描述候选状态。需要用户在下一轮实现前确认是否接受这一语义；若坚持phase必须同步变化，采用第二路线，预计影响面更大。

### 最小增量结构与接口

[INFER] 建议一个013增量迁移新增两个控制表，不复制源／Topic／目标端点。下列是设计字段，不是本轮已落库结构：

| 建议记录 | 必需内容 | 作用 |
| --- | --- | --- |
| API读选择单例 | 当前version_id、contract_sha256、目标handoff引用（legacy可空）、递增read_revision、当前operation_id、updated_at | 一个事务同时选中ES/CK和状态范围；所有API启动／请求核对同一记录 |
| 读切换操作 | operation_id、PROMOTE/ROLLBACK、from/to版本及摘要、expected_read_revision、freeze_id/revision、rollback_of、阶段、revision、last_error | 幂等CAS、序列化并发操作者、崩溃后确定切换是否提交 |
| 操作中的新鲜证明 | marker_id/issued_at、源／Topic身份引用、每分区标记位置／实际提交／start/end、owner epoch及组成员观察、完整报告及hash、验证时间 | 证明与本次冻结和目的版本绑定，保留失败／过期记录 |
| 操作中的API观察 | 旧API退出证明、预期新read_revision、新boot_id/host/PID/监听观察、就绪及两条API验证摘要 | 区分配置已提交和真实API就绪；只是当次观察，不永久等同存活 |

源及目标身份始终从不可变Version/Handoff解析；组也从legacy binding或B的固定group解析，避免另复制可漂移配置。证明和动态进程观察属于操作记录，可用JSON保存，不进入Contract指纹。单例记录及操作行采用既有MySQL事务和显式行锁；短操作锁可借用StateStore.Lock的方式。Begin应在同一事务锁住冻结闸门与读选择单例，拒绝其他未完成操作，并登记PLANNED和当前operation_id；此时仍保持from和原read_revision。这样进程崩溃不丢失准入关闭状态，并发控制器也不能各自登记一轮切换。

最少四个持久阶段足以表达用户要求的恢复状态：

| 阶段（建议） | 当前读选择／闸门 | SIGKILL后该做什么 |
| --- | --- | --- |
| PLANNED | 读选择仍from，业务FROZEN；证明／退出字段可能未完成 | 未切换。读取已持久标记／状态，重做未完成证明和退出观察；过期证明不能直接提交 |
| COMMITTED | 读选择已to，read_revision已增长，业务FROZEN | 已切换但未就绪。禁止重新提交旧revision；只允许启动／核验匹配to的新API，或另建逆向回滚操作 |
| API_READY | 新API当次启动／查询核验通过，业务仍FROZEN | 再查当前进程／read_revision和状态，显式Resume；历史ready记录不能代替当前存活 |
| RESUMED | 闸门OPEN且恢复日志、操作状态同一事务提交 | 业务已恢复。若以后失败，重新冻结并新建操作，不倒退此记录 |

PLANNED→COMMITTED的事务同时验证确切freeze、预期旧选择、目的合同／新鲜证明及API排空，再更新单例、阶段和控制日志。事务结果未知时只读回operation和单例：仍from表示未提交，to加该operation／revision表示已提交；其他值应保守拒绝并发覆盖。

API_READY→RESUMED必须把闸门开启、操作完成、恢复日志放在一个MySQL事务中。当前 [Resume:151](../internal/projection/gate.go#L151) 只检查未VERIFIED的B交接；B已完成后它不会等待API切换或新API就绪，因此必须添加新操作守卫和新元数据缺失拒绝。如要放弃尚未提交的PLANNED，需一个显式、校验读选择未变的取消动作；已COMMITTED只能走新逆向操作，首版不做自动回滚。

[INFER] 建议操作接口为 BeginReadSwitch、ValidateDestination、CommitReadSelection、ObserveAPIReady、ResumeReadSwitch、ReadSwitchStatus；回滚使用相同接口并指定ROLLBACK和历史rollback_of。API解析接口为 ResolveReadSelection/CheckReadSelection，进程固定version+read_revision，环境配置只是期望值，数据库记录才是选择来源。沿用原维护CLI和独立控制DSN，不增普通用户可调用的维护HTTP入口。

## E. 推广前新鲜验证的复用点

[CODE] B的Verify确实完成过新标记、两组跨分区提交和完整内容对账；但一旦进入VERIFIED，[target_status.go:176](../internal/syncpipe/target_status.go#L176) 只检查身份就返回旧记录。原冻结ID/revision也被固化在Handoff中（[checkFrozen:65](../internal/syncpipe/handoff_control.go#L65)），恢复业务再冻结得到的新freeze不能重用旧交接。下一轮要提取可复用的证明步骤，保留B本身的终态和旧证据。

| 新证明步骤 | 可以复用的当前symbol | 需要的最小补充 |
| --- | --- | --- |
| 在新冻结下发精确标记 | [StateStore.IssueWatermark:11](../internal/syncpipe/watermark.go#L11)、[Rebuilder.barrier:57](../internal/syncpipe/rebuild_tail.go#L57) 的原SQL／事务模式 | 新入口把marker ID与切换操作一起持久化并返回，不只读源水位最新行 |
| CDC确认所有分区发布 | [Capture事务发布:177](../internal/syncpipe/capture.go#L177)、[Queue.PublishBarrier:266](../internal/syncpipe/queue.go#L266) | 不改协议／检查点；观测同一marker在全分区的实际记录及消费者持久处置 |
| 候选组逐分区确认 | [committedProgress:74](../internal/syncpipe/target_consumer.go#L74)、[Verify标记等待:215](../internal/syncpipe/target_status.go#L215) | 按本操作marker查delivery，不用历史verification_offset；保存原始FetchOffsets和范围 |
| 身份与进度 | [identities:26](../internal/syncpipe/projection_control.go#L26)、[checkIdentities:75](../internal/syncpipe/handoff_control.go#L75)、[rawOffsets:291](../internal/syncpipe/handoff_control.go#L291) | 同时查当前源／Topic UUID／分区集合、ES/CK UUID及各组当前位置；不初始化位置 |
| 当前候选writer及失败 | [TargetConsumerConfig.Check:49](../internal/syncpipe/target_consumer.go#L49)、[TargetStatus:19](../internal/syncpipe/target_status.go#L19)、[Verify最终核验:284](../internal/syncpipe/target_status.go#L284) | 保存当前owner epoch、锁连接、group、近期heartbeat、lag、单成员Stable及无未完成／隔离；提交前再核对 |
| 源和两投影完整比较 | [Reconcile:184](../internal/syncpipe/reconcile.go#L184) | 目的版本的真实ES/CK客户端；组织范围0全量，源before/after相同、差异0、冲突0，可附M3 Gold指标 |
| 绑定并提交证明 | [HashJSON:20](../internal/projection/store.go#L20)、控制日志事务方式 | 报告hash、freeze ID/revision、目的版本/hash、marker向量与read_revision关联新操作 |

[CODE] IssueWatermark只返回error，CDC又每2秒更新源水位（[capture.go:240](../internal/syncpipe/capture.go#L240)）。从最新水位行再读ID可能读到别的控制标记；该共享行只是“最新值”，不是推广操作的不可变标记账本。建议借用M4 barrier在同一事务写任务与marker的方式，为操作保存确切ID，继续让CDC从原Binlog读取并发布，不直接往Kafka冒充源边界。周期标记可以继续，不需停CDC。

[CODE] CDC只有整笔源事务全部发布确认后才 [SaveCheckpoint:210](../internal/syncpipe/capture.go#L210)；发布全分区标记时也遵守此规则。B新组delivery中的相同marker已applied，且每分区committed严格大于消息Offset，是实际确认；仅看到单个分区、只看lag=0或水位时间都不足。

[CODE] Reconcile开启MySQL RR只读视图，比较ES当前文档及external version、CK规范当前／历史，并独立再次读取源摘要（[reconcile.go:214](../internal/syncpipe/reconcile.go#L214)、[242](../internal/syncpipe/reconcile.go#L242)、[271](../internal/syncpipe/reconcile.go#L271)）。q=nil的调用把边界标为caller_confirmed，第224行不替操作者确认消费；因此必须先完成真实marker／提交证明，或传入正确的ConfirmSourceBoundary回调。不能单独执行q=nil完整对账后就宣布消费已追平。

[INFER] 新鲜证明提交期间要持有新冻结的锁保护，且新待切换操作必须跨控制器崩溃持续阻止Resume。证明过期、freeze revision变化、owner重启、Topic／目标变化或未处理失败出现时重新证明；不把固定20秒等经验值当成安全保证。真正耗时和期限需下一轮真实环境测量。

### 旧目的投影能否做同等级证明

[CODE] 原组、旧delivery、水位及CK状态仍保存，Reconcile的ES/CK参数可以指向旧目标；它并不要求必须是候选。旧组原始FetchOffsets和ListStart/EndOffsets也可以调用。因此可以实现同一份目的投影验证器的legacy分支，使用旧组和旧表确认确切新标记，再全量比较旧目标。

当前还不能直接复用B的候选验证器：legacy没有B的handoff/pinned/owner namespace；旧SearchFreshness也不证明各分区追平。`Queue.Progress` 在旧组缺提交时回退log_start（[queue.go:342](../internal/syncpipe/queue.go#L342)），下一轮回滚证明必须用原始FetchOffsets先要求每分区都存在，不能用这个回退计算掩盖缺失。允许committed==log_end；小于log_start或大于log_end都拒绝。已赶上的组不要求历史初始pinned仍被保留，验证的是当前位置和新边界。

[INFER] legacy writer没有012的目标lease。首版同机回滚应限制其为明确登记的单个ES／CK工作进程，核对实际启动配置、二进制和组成员，再确认其新标记、心跳和完整内容；不把单一组件的共享heartbeat当成严格进程所有权。若用户要求旧投影也具备与候选相同的持久lease，需要另一个明确的旧writer登记适配，不能在读切换时直接拿B的候选owner去覆盖它。

## F. 旧目标继续追平与回滚条件

[RUNTIME] B主链保留原ACTIVE和候选同时消费，显式Resume后真实HTTP评论使两套ES／CK都到工单v11，候选全量10工单／47历史仍consistent；原版本保留断言见 [m403b测试:316](../internal/mysqlstore/m403b_handoff_integration_test.go#L316)，HTTP写入及双目标v11断言见 [335](../internal/mysqlstore/m403b_handoff_integration_test.go#L335)、[345](../internal/mysqlstore/m403b_handoff_integration_test.go#L345)，结果见 [after-resume.json](../artifacts/m4-03b/runs/20261114T104130.415828Z-focused/verification/TestM403BOfflineNewGroupsExactPositionsAndGold/after-resume.json)。这证明“两套不同目标可以并行追平”的一次隔离场景，未证明真实推广后的旧组生命周期。

[CODE] 两组在不同索引／CK空间写入；候选lease以component+endpoint+namespace计算物理key（[writer.go:22](../internal/projection/writer.go#L22)）。候选组从Handoff固定，legacy组为topic.es.v1/ck.v1（[queue.go:296](../internal/syncpipe/queue.go#L296)）。状态以VersionID分流，省略handoff写同一候选会被拒绝（[rejectUnmanagedCandidate:130](../internal/syncpipe/target_consumer.go#L130)）。API读选择本身不需要改变这些写入归属。

旧消费者和共享CDC是否能继续，取决于选择哪条推广路线及实际启动方式：

| 当前依赖 | [CODE]位置 | 下一轮必须保留／调整的条件 |
| --- | --- | --- |
| legacy消费者带managed.Check | [cmd/ticketpulse-sync/main.go:165](../cmd/ticketpulse-sync/main.go#L165)、[187](../cmd/ticketpulse-sync/main.go#L187) | 现有检查要求旧登记仍ACTIVE、全局binding仍旧；读选择器路线可保留，改phase路线必须拆出writer自身目标检查 |
| 共享CDC也带managed.Check | [cmd/ticketpulse-sync/main.go:164](../cmd/ticketpulse-sync/main.go#L164)、[Capture.Run:107](../internal/syncpipe/capture.go#L107) | 如果改旧ACTIVE或旧投影UUID失效，该检查会阻止CDC；不能在读切换时连带停止共享发布 |
| 候选消费者依赖PREPARED+VERIFIED | [handoff.go:67](../internal/projection/handoff.go#L67)、[Target.Check:49](../internal/syncpipe/target_consumer.go#L49) | 不改Handoff摘要、phase及group；回滚读目标不需重初始化或封存候选writer |
| legacy元数据缺失兼容 | [HandoffInstalled:168](../internal/projection/handoff.go#L168) | 新读功能一旦安装，也应拒绝缺失读选择器；不要因缺表重回未管理API |

[UNKNOWN] 原演示和未来推广实例究竟以managed、legacy或all进程方式运行，本轮未查询进程配置。若使用all共进程，停止该进程会同时停止CDC／两个worker；不能把它作为“只停止旧API／只停止旧投影”的操作。API本身为单独二进制，受控API重启不需要杀sync进程。

[INFER] 推荐保留两套投影在整个回滚窗口继续消费。回滚前重新冻结、登记逆向操作，目的为旧投影；用新的同等级证明通过后，按C的同一顺序停止当前API、切回读选择、增长修订号、启动旧目标API并显式Resume。正向与逆向都不重置任何组Offset，不用Handoff.Init，不让M4 follower重新进入已经封存的目标。

在读选择器路线下，回滚后新目标保持PREPARED／Handoff VERIFIED，标准writer继续追平，不需额外版本phase转换；正向、逆向历史都在新操作记录中保存。如果选择phase更新路线，回滚必须在同一事务把版本角色和状态引用再次切换，并允许“原legacy合同作为备用角色”；这一能力当前尚不存在。

| 回滚目的投影的检查 | 满足才可回滚 | 应明确拒绝的情况 |
| --- | --- | --- |
| 身份与完整分区 | 源／Topic UUID、分区、ES/CK UUID均与记录一致 | 同名重建、部分表缺失、端点不明或查询失败 |
| 原始提交位置 | 每个分区都有提交，start≤committed≤end，越过本次marker | 组被删／过期、任何分区缺失、位置过期或越界 |
| 当前进程与状态 | 受控writer、近期心跳、真实组成员及状态范围匹配 | writer来源不明、旧heartbeat被误当新组、未证明旧进程归属 |
| 失败／隔离 | 无未解决终态失败、隔离或处理中缺口 | 即使lag为0仍有隔离／无法处理记录 |
| 全量内容 | 源前后相同、ES/CK完整一致、规范冲突0 | 旧目标漏增量、正文／状态损坏、同键冲突、源仍在变化 |
| API与冻结 | 目的读选择／修订号匹配、旧API退出、新API就绪、同一freeze | 未排空、错误启动、提前Resume、证明属于旧冻结 |

[INFER] 前向推广是否要求旧投影也提前证明可回滚，需要用户确定：强制两套都健康会阻止“旧投影坏了、候选恢复正确”的推广。最小实现可以只强制目的投影证明，同时明确记录回滚当前可用／不可用；每次实际回滚仍重新证明旧投影。不能承诺旧目标永远可回滚。

本轮没有授权停止原投影。未来停止旧消费应作为回滚窗口关闭后的显式操作：确认新读目标已验证且稳定、用户接受失去即时回滚、保留所有旧数据／组／证据，并确认共享CDC不会因旧目标检查被停止。旧组暂停后只有在Kafka仍覆盖未读位置并能重新追平时才可能恢复回滚；保留范围不足应拒绝并另走重建，不能移Offset跳过。

## G. M4-03C最小实施文件、接口与测试矩阵

以下均为[INFER]下一轮实施建议，本轮未创建这些源码、DDL或测试。推荐分成三步，首步不提交读选择，第二步才完成真实推广，第三步验收回滚。每步有可独立核验结果，避免把“新API能启动”直接当成推广通过。

| 步骤 | 最小实现 | 验收出口 |
| --- | --- | --- |
| 1 持久选择／新鲜证明基础 | 新单例与操作模型、CAS／Resume守卫、目的投影legacy/handoff解析、精确标记和全量证明 | 在独立环境生成绑定新freeze的新证明；拒绝旧VERIFIED、错配、缺提交和未完成Resume；尚不切读 |
| 2 正向推广与受控API重启 | 原子提交读选择、固定API read_revision、两数据客户端与状态读器接线、请求校验、排空／就绪观察 | 同一维护窗口内完成old→candidate；真实Search/Analytics读候选状态，业务恢复后继续正确 |
| 3 逆向回滚与失败恢复 | 新冻结、旧投影同等级证明、逆向选择CAS、修订号增长、明确拒绝不可用回滚 | new→old→new重试及各SIGKILL边界可恢复，所有组Offset单调接续，旧游标不复活 |

建议影响面如下；命名是候选，不要求另起大型配置中心或改M4恢复协议。

| 文件／接口 | 责任 | 范围限制 |
| --- | --- | --- |
| 新 db/migrations/013_projection_read_control.sql、db/embed.go | 两张读控制表及显式安装记录 | 不改业务／原组Offset／原A/B Contract JSON |
| 新 internal/projection/read_control.go、read_switch.go | 选择单例、操作阶段、CAS、证明保存与崩溃读回 | 与Contract分离，不把动态值放指纹 |
| internal/projection/gate.go | 未完成读切换／元数据缺失拒绝Resume；完成与OPEN原子提交 | 保持业务事务FOR SHARE排空机制 |
| 新 internal/syncpipe/read_projection.go、read_switch_control.go | legacy/handoff状态解析、只读身份、精确新marker／当前位置、当前writer／失败／全量审计 | 复用B内部检查，保留VERIFIED历史，不重新Init或启用follower |
| internal/syncpipe/watermark.go | 小幅增加“指定／返回精确marker并绑定操作”的事务辅助 | CDC事件格式、发布ACK及源检查点合同不变 |
| cmd/ticketpulse/main.go | 从一个持久读选择构造两个客户端和两状态读器，核对启动配置及修订号 | 普通未启用模式兼容；已启用不可省略受管理检查 |
| internal/search/service.go及目标游标测试 | Target增加读启用修订号，与版本/hash结合 | 不改过滤、授权、排序或2000候选上限；期限400与跨目标409分别保留 |
| internal/httpapi/session.go／api.go（如必要） | 受管理读准入与操作者可核对的启动／修订号观察 | 不新增维护写HTTP API，不暴露端点／凭据／正文 |
| cmd/ticketpulse-control/main.go | plan/validate/commit/status/observe-ready/resume及显式逆向操作 | 同一op安全重试，结果未知先读持久状态 |
| 新 scripts/m403c_*、隔离integration测试 | 单机API进程身份、退出／启动验证、故障矩阵和资产核验 | 只控制新私有API与数据库／目标，独立端口，无通用pkill |
| cmd/ticketpulse-sync/main.go／投影检查适配（条件项） | 只有实际managed CDC依赖读ACTIVE、或选择改phase路线时才拆读／写／源检查 | 不重构发布协议，不把共享CDC停掉来制造切换成功 |

必须验收的场景如下。除纯模型／游标编码外，表中涉及切换、就绪、同步和故障的项目都须使用独立真实MySQL、Kafka、ES、CK和API进程；本轮没有新PASS结论。

| 场景 | 注入／输入 | 预期观测 |
| --- | --- | --- |
| 兼容与启用 | 无013的legacy；启用后缺选择／错配置／试图省略参数 | 前者原回归正常，后者启动或读请求拒绝；无默认目标回退 |
| 新冻结证明 | B已VERIFIED后发生业务写入，再生成新freeze | 旧报告拒绝；新marker全分区提交并完整对账后才可提交选择 |
| marker交错／跨分区迟到 | 周期水位覆盖最新行，某分区延迟、CDC事务未全部ACK | 精确操作marker不变；不凭最新水位或部分分区允许推广 |
| 候选或旧组缺口 | DeleteOffsets、start推进、越界、稳定组／writer状态异常 | 目的投影证明失败；读选择／freeze保持；不初始化旧／新Offset |
| 隔离／内容冲突 | lag=0但隔离、ES错版本、CK同键冲突、漏历史 | 全量及终态检查拒绝；状态不能复制旧caught_up |
| 数据与状态一致选择 | 两投影保留可辨认状态，读选择指向候选 | Search freshness、Analytics Sync组／版本与真实数据客户端同源 |
| 未管理旧API及多进程 | 两个不同端口API、遗漏进程、旧配置启动或自动重启 | PLANNED关闭新投影读及旧版本启动准入；未证明全部受控旧进程结束不能提交；旧二进制自动重启入口须已停用 |
| 校验后在途请求 | 暂停已通过旧check的Search或Analytics，开始停止流程 | 切换等待排空／进程退出；提交后旧进程不能再生成成功响应，先前已发送的数据不计入此保证 |
| API优雅退出超时 | 活跃请求长于5秒；SIGTERM后Timeout／SIGKILL | 不能以Shutdown返回超时当成功，需实际退出和端口证明 |
| 控制器SIGKILL | 新marker事务前后、proof保存前后、选择事务前后 | PLANNED/COMMITTED正确区分；重试不重复切换、不恢复写入 |
| 选择已提交、API启动失败 | 错端点／缺密钥／UUID变化／API SIGKILL | COMMITTED+FROZEN；只启动匹配修订号API或显式逆向，不盲目Resume |
| 新API就绪后SIGKILL | 保存API_READY后进程终止 | ready记录不能代替当前观察；Resume核验失败，保持冻结 |
| Resume结果未知 | OPEN事务前后控制器SIGKILL | 闸门、操作RESUMED和日志一致；新freeze不能重试旧Resume |
| 并发操作者 | 两个不同from/to、重复同op、错误expected_revision | 单例CAS拒绝冲突；同op重试依据持久结果 |
| 当前权限与Gold | 原十工单、转派／成员变化、注入重复／旧版本 | MySQL最终授权、字段版本、M3统计及全量对账保持一致 |
| 实际回滚 | 推广后新增业务，两套继续追平，再新freeze回滚 | 旧目的也越过新marker且全量一致；不靠旧B报告或Offset重置 |
| 回滚拒绝及窗口关闭 | 旧目标UUID变、旧组过期、停旧writer、证据未知 | 明确ROLLBACK_UNAVAILABLE／验证失败；保留当前读选择和资产 |
| 游标A→B→A | 同密钥、未到期旧A／旧B游标、真的到期游标 | 跨启用修订号409；旧A不复活；期限无效仍按既有400合同 |
| 资产与回归 | M1–M5相关回归、A/B原专项、历史文件哈希 | 只新目录追加；原演示端点／组／数据／证据不变 |

下一轮实际使用的命令必须经过新私有资源核验，再创建测试源、Topic、两个投影和服务。不要复用B随机夹具资源名，也不要为“验证读选择”指向演示端口。

## H. 未解决的阻塞问题及结论

下列是实施前必须明确的决策／环境事实，不妨碍本轮源码调查完成：

| 待确认项 | 为什么影响最小实现 | 默认建议 |
| --- | --- | --- |
| API读ACTIVE是否与登记phase分离 | 决定是否必须修改Version.Validate、Handoff和legacy worker/CDC依赖 | 独立读选择器为唯一API真相；保持A/B合同 |
| 受控API范围、监听入口、进程管理方式 | 当前没有持久API身份／单例保护；不能证明普通重启排空所有读者 | 首版同机单个受控API，记录PID+boot nonce与端口，退出不明拒绝提交 |
| 实际源和sync启动模式 | managed all的旧目标检查可能连带停止CDC | 核对独立实例；API与sync分开控制，共享CDC持续发布 |
| legacy writer是否要求012式持久lease | 旧状态／组可证明投影一致，不提供候选同等级进程owner | 首版明确单机受控进程；如要求严格lease另补适配并验收 |
| 前向推广时必须保证旧目标可回滚吗 | 旧目标坏是重建的重要场景，强制旧健康会阻止恢复推广 | 强制目的健康；单独报告回滚当前可用性，每次回滚重新证明 |
| 回滚窗口与旧消费停用条件 | 暂停后日志保留和组提交过期可能让回滚不可用 | C首版保持两套消费与全部资产，窗口关闭另行明确授权 |
| 游标过期错误语义 | 当前真的过期400、有效跨目标409；不应无意改合同 | 保留期限语义，新增read_revision阻止A游标复活 |
| 真实DDL、组位置、owner、容量期限 | 已有报告是历史；私有B服务已停，本轮不连接／启动 | 下一轮独立核验并执行新标记／完整对账，不能直接复用旧PASS |

[UNKNOWN] 本轮没有实时数据库、Kafka、ES/CK或API进程观察，也没有执行C的故障实验。旧演示迁移、活跃进程和今日同步状态只能保持未知，不能从历史截图或报告推断。多Broker新读切换、十万级推广耗时、跨主机／网络fencing、热切换和自动回滚均不在本轮已验证范围。

四个最终问题的回答：

1. 当前源码不足以直接完成安全受控读目标切换。基础机制可复用，但API仍只能接受首次登记ACTIVE，候选状态未接入API，没有持久读切换／API恢复阶段，新鲜证明也不能靠重复B Verify取得。
2. 最小缺口是两个小控制记录（统一读选择与可恢复操作）、目的投影的精确新冻结证明、API数据与状态统一解析／请求校验／启用修订号、实际排空及就绪观察、Resume守卫。A/B的Contract、owner、group和共享CDC检查点可以保留。
3. 最少三步：先实现控制与新证明基础，再验收正向持久切换和受控重启，最后验收新冻结下的逆向回滚及SIGKILL恢复。若坚持修改现有ACTIVE phase，先完成明确的合同兼容调整，不能混在裸SQL切换中。
4. 新标记全分区发布／提交、目的投影全量审计、状态接线、在途读排空、进程SIGKILL、选择事务及Resume恢复、旧组缺失／保留失效、双投影新增写入、真实回滚和跨启用游标行为，都必须在完整独立MySQL/Kafka/ES/CK/API环境验收。纯单元或当前源码可行性不能代替这些真实结果。

本报告交付的是实施建议和待确认边界，没有开始M4-03C开发或实际切换。
