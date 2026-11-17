# TicketPulse M4-03C2：持久读目标推广与受控 API 切换

[RUNTIME] 最终状态为 M4-03C2_PASS。独立真实环境已经完成A→B读目标提交、新API就绪和显式恢复业务；14项C2专项、40项A/B/C1专项回归、89项M1–M5业务/同步回归、Go race、go vet和3项Python测试通过。登记ACTIVE/PREPARED保留原含义，API读目标由013单例决定。完整验收与保留核验见 [test-summary.json](../artifacts/m4-03c2/test-summary.json) 和 [closeout.json](../artifacts/m4-03c2/closeout.json)。

证据标签：[CODE] 为重新核对的源码；[RUNTIME] 为真实执行；[INFER] 为边界判断；[UNKNOWN] 为尚未验收。历史报告只用于定位。

| 术语 | 本轮含义 |
| --- | --- |
| A / B | 原登记ACTIVE投影 / M4-03B已接续的PREPARED候选 |
| 读选择 | 013单例同时决定ES、CK、Search freshness和Analytics status |
| 读修订号 | read_revision，正向推广从1增加到2，仅成功提交一次 |
| boot | 本次API启动ID，加主机、PID、启动时间、可执行哈希和监听地址 |
| 排空范围 | 同机操作者持有的可执行文件清单与已停用重启入口 |

## 1. 基线与实际改动

[CODE] 分支为 feat/m1-01-ticket-loop，仓库没有有效HEAD，index为空。实施前独立归档273个文件，与C1最终273文件全部匹配。最终288个文件，15个新增、14个修改，没有删除。基线、原字节和实施前调用链分别见 [baseline-source-manifest.json](../artifacts/m4-03c2/baseline-source-manifest.json)、[baseline-source.tar.gz](../artifacts/m4-03c2/baseline-source.tar.gz)、[preflight-source.json](../artifacts/m4-03c2/preflight-source.json)、[source-facts.json](../artifacts/m4-03c2/source-facts.json)；新增接线的准确symbol、行号与哈希见 [final-source-facts.json](../artifacts/m4-03c2/final-source-facts.json)。7,248个已有证据文件另行保存SHA-256；没有以git diff为空判断无改动。

| 文件 | 职责 |
| --- | --- |
| [014_projection_read_runtime.sql](../db/migrations/014_projection_read_runtime.sql)、[db/embed.go](../db/embed.go) | 两张增量运行证据表，显式安装；011–013保留原字节 |
| [projection/read_execution.go:29](../internal/projection/read_execution.go#L29) | 安装、完整执行证据及摘要、阶段事务日志和关联验证 |
| [projection/read_api.go:53](../internal/projection/read_api.go#L53) | 启动boot登记、心跳和每请求持久准入 |
| [projection/read_control.go:183](../internal/projection/read_control.go#L183) | 合法读回初始A与一次已审计正向提交B；普通Resume守卫继续生效 |
| [readruntime/process.go:60](../internal/readruntime/process.go#L60) | macOS同机PID、启动时间、可执行哈希、监听及重启入口观察，不发信号 |
| [read_api_drain.go:58](../internal/syncpipe/read_api_drain.go#L58) | 持久进程清单和退出观察，提交前再次核对受控旧API |
| [read_switch_commit.go:27](../internal/syncpipe/read_switch_commit.go#L27) | 新鲜重验和原子选择CAS；未知结果读回与幂等重试 |
| [read_api_selection.go:19](../internal/syncpipe/read_api_selection.go#L19) | 统一目的读解析；环境变量只核对预期，不覆盖选择 |
| [read_api_ready.go:82](../internal/syncpipe/read_api_ready.go#L82) | 真实HTTP、当前授权、精确marker及完整对账；就绪和显式恢复 |
| [read_switch_control.go](../internal/syncpipe/read_switch_control.go) | 抽取C1既有证明逻辑复用，状态显示执行证据；精确marker协议保留 |
| [ticketpulse/main.go:138](../cmd/ticketpulse/main.go#L138) | 选择器启动接线、boot及身份响应头、Shutdown和超时关闭连接 |
| [ticketpulse-control/main.go](../cmd/ticketpulse-control/main.go) | 操作者CLI，没有普通用户维护写API |
| [httpapi/api.go:74](../internal/httpapi/api.go#L74)、[session.go](../internal/httpapi/session.go) | 当前主管授权的只读目标诊断，原业务授权保持 |
| [search/model.go](../internal/search/model.go)、[analytics/model.go](../internal/analytics/model.go) | 受管理状态附加版本、修订号、namespace和组身份 |
| [read_revision_test.go](../internal/search/read_revision_test.go) | 同Contract不同修订号、A→B→A预留、过期和密钥错误定向验证 |
| [m403c2_read_switch_integration_test.go](../internal/mysqlstore/m403c2_read_switch_integration_test.go) | 全进程正向切换、故障、权限、Gold、位置和恢复证据 |
| 既有MySQL/A/B/C1/reconcile集成测试入口 | 增加C2私有端口/socket准入及可选透明故障代理/进程夹具，保留原断言 |
| scripts/m403c2_environment.py、m403c2_suite.py、m403c2_preservation.py、m403c2_package.py | 私有实例、预算、整轮互斥、保留核验和逐字节包装 |

完整变更见 [file-manifest.json](../artifacts/m4-03c2/file-manifest.json)、[full.diff](../artifacts/m4-03c2/full.diff)、[final-source.tar.gz](../artifacts/m4-03c2/final-source.tar.gz)。旧/新字节保存在source/original和source/final。原Version模型、Handoff、writer ownership、标准consumer、Kafka queue、CDC capture、Reconcile、M3统计SQL及011–013DDL保持基线字节。

## 2. 持久模型、阶段与请求准入

[CODE] 014增加 sync_projection_read_execution 和 sync_projection_api_boots。前者按operation保存进程清单、退出证明、提交、就绪、恢复JSON及独立摘要和修订号；后者保存boot不变身份、状态、心跳、Shutdown结果和退出时间。安装014要求013已显式安装，不自动初始化选择，不迁移业务数据或Kafka位置。未启用013时保留原API/Resume方式；启用后缺行、坏摘要、丢表、非法阶段或缺managed参数保守拒绝。

选择器仍是一条version/hash/handoff/revision记录。C2只实现一次PROMOTE，revision1→2，B保持PREPARED。合法读回需要原enable日志、本次完整提交日志、当前Handoff和分区确认；只用SQL修改stage或selector不构成成功合同。boot、heartbeat等动态值只写014，不进入Contract指纹。

| 持久阶段 | 投影读取准入 | 业务写入与恢复 |
| --- | --- | --- |
| 无操作、A/revision1 | 配置和身份一致的A可启动和请求 | 原闸门合同 |
| PLANNED | 新Search/Analytics和新API boot拒绝；已准入请求必须排空；详情/基础列表仍读MySQL | FROZEN，普通Resume拒绝；合法取消按C1原条件恢复 |
| COMMITTED | 只允许当前B/hash/revision2的新API真实读验证，旧A配置拒绝 | 继续FROZEN，不自动切回或Resume |
| API_READY | 同一B；历史ready不能替代当前存活和真实探针 | 显式ResumeReadSwitch重新核对当前boot |
| RESUMED | 固定B/hash/revision2，每请求核对持久选择和阶段 | OPEN和RESUMED同事务，普通Resume不能绕开未完成操作 |

[CODE] 正式提交锁顺序为闸门FOR UPDATE→选择器FOR UPDATE→操作行→执行证据。Begin/取消保持C1冻结及CAS；API启动和请求在选择器FOR SHARE保护下完成准入。就绪/恢复持有冻结及选择共享锁，允许真实HTTP请求读取；恢复在探针结束后升级闸门排他锁。

[INFER] 进程停止和数据库事务不能成为跨系统原子操作，所以操作者独占同机启动范围是必要前提；代码没有扩展为动态配置平台。

## 3. 新鲜重验与原子选择提交

[CODE] CommitReadSelection不依据旧consistent字段放行。它重新匹配PLANNED、确切冻结、from/hash/revision、候选Handoff、源/Topic/ES/CK UUID及本操作proof，随后复用C1证明：同一精确marker的真实Kafka消息、两组全部分区实际committed、保留范围、当前owner/epoch/lock/heartbeat/单成员Stable、零未完成/隔离和完整MySQL/ES/CK对账。当前源摘要必须仍等于C1冻结证明；不能换成未经重验的writer。审计后再次核对动态身份、位置和旧API实际退出。

MySQL一个事务保存重验后的完整证明、提交和退出审计，CAS更新A→B、revision+1、operation=COMMITTED。没有分别提交ES/CK，没有OffsetCommit、初始化、复制进度、修改源checkpoint或重启封存follower。相同operation重复Commit读取B/revision2及对应证据，不增加第二次revision；组合不一致拒绝。

[RUNTIME] 主流程确认A提供Search/Analytics且B VERIFIED，再HTTP评论工单4到新版本，二次冻结并生成操作专属marker。新证明及提交时重验对应10工单/47历史，首次解决6、重开2，源前后相同，ES/CK差异和冲突为0。实际消息与组读回见 [proof-vector.json](../artifacts/m4-03c2/proof-vector.json)，提交前完整报告见 [fresh-proof.json](../artifacts/m4-03c2/fresh-proof.json)。没有从周期最新watermark推断marker身份。

最终主流程的新标记为 ae7a80a03dce4ccbe90480f99755c941，绑定冻结5c8c05eeabe7c333fe99ab5cdf45587d/revision3、操作forward_live和读revision1。下表是Commit重验后的原始读回；Kafka提交位置表示下一条消息，所以必须严格越过对应标记消息位置。两组独立确认，没有写入或重置Offset。

| 分区 | 本次标记消息Offset | ES组committed | CK组committed | 保留范围[start,end] |
| --- | --- | --- | --- | --- |
| 0 | 37 | 39 | 39 | [0,39] |
| 1 | 32 | 34 | 34 | [0,34] |
| 2 | 25 | 27 | 27 | [0,27] |

证明SHA-256为91cb2b6ea157c98d4c88a0629284edfdebdd36d5b15e6d00b4756944b0688736，完整对账报告SHA-256为fd80993d6378d9368bef572336ee42b3ac9f99a12f9bec26fba06d7a3019dde7。标记的实际消息、原始FetchOffsets响应、当前owner和报告全部随证明保存。

## 4. 旧API排空与同机保证

[CODE] 清单固定host、API可执行路径/hash、非API角色/hash、直接launcher身份及已停用launchd标签/配置摘要。范围内未声明进程、未知launcher、重启入口仍加载、身份不明或文件变化拒绝提交。旧C1二进制不认识013读选择，必须实际退出；代码没有声称新表能约束它。已登记API还与当前PID、启动时间、监听和boot匹配，无法确认的进程保持冻结。

操作者显式停止清单中的API。服务先记录DRAINING并调用Shutdown关闭监听，等待在途请求；超时显式Close连接。只有PID实际不存在、受控进程重新枚举为空，控制器才保存退出证明，Commit再次观察。SIGTERM成功、端口关闭、readyz503、固定等待时间或named lock都不作为退出证明。

[RUNTIME] 两端口实验启动新版API和基线C1旧二进制。PLANNED后新版Search拒绝，旧二进制仍可读A；停止一个仍拒绝排空和Commit，两个实际退出后才提交。在途Search通过真实ES透明代理，暂停已通过准入的请求；正常Shutdown等待完成，100ms超时场景记录强制连接关闭并确认PID退出。旧API和API_READY后的SIGKILL都有实际进程退出观察。清单、退出时间和监听见 [api-drain.json](../artifacts/m4-03c2/api-drain.json) 与故障矩阵。

[RUNTIME] 真实私有launchd KeepAlive入口加载时拒绝，按精确私有label执行bootout后才登记停用配置；无关系统入口仅只读，原演示未停用。运行器互斥锁阻止另一轮构建覆盖仍在使用的二进制；相关早期失败日志保留。

[INFER] 保证限于macOS同机、合作进程和操作者独占的可执行/启动清单。没有证明任意未登记别名、副本、root级手工启动、其他监督平台或跨主机进程。任意SQL/不合作ES/CK writer和请求级fencing在范围外。重启标签清单完整性由操作者负责，未知监督方式拒绝，不自动接管。

## 5. API统一接线、游标与就绪恢复

[CODE] API启动读取数据库权威选择，环境ES/CK/版本仅核对预期；B选择但配置A、省略managed、CK错空间、UUID错或坏控制记录拒绝。PREPARED读检查按B Handoff，不能用原ManagedProjection的phase=ACTIVE条件替代。A使用原Search freshness/AnalysisStatus，B使用既有TargetStatus及两个候选组，数据和状态同时由一个SelectedAPI解析。

每个新Search/Analytics执行持久选择/revision/boot检查，保留原ES过滤、MySQL当前权限和版本比对、排序/search_after、page_size+1和2000候选预算。详情/基础列表仍用原MySQL快照授权；Analytics主管授权和M3规范事实SQL保持原实现。新增主管只读 /api/v1/read-target 和身份响应头，无公开维护写路由。状态附加读版本、revision、namespace和group，以实际读器输出为准。

游标target纳入read_revision，Contract.Fingerprint不变。有效A游标用于B返回409 SEARCH_CURSOR_TARGET_CHANGED；真正过期和密钥错误保留400。A→B→A相同Contract的新revision不复活旧游标有单元验证，真实逆向流程留C3。

[CODE][RUNTIME] ObserveAPIReady核对当前boot的PID/监听、可执行身份、存活和heartbeat，真实请求readyz、read-target、主管Search/Analytics及普通用户权限探针。Search首屏与独立MySQL当前权限页比较字段、版本、ID排序和has_more，验证扫描仍有2000上限；Analytics完整对账附独立MySQL指标并比较真实概览。状态必须来自B组/namespace，精确marker仍全分区确认。十工单Gold来自冻结PRD文件，不用正式指标函数生成。

最终主流程的新API boot为573f2eefdabf0ee80c9a133259ebe905，监听127.0.0.1:55178，固定candidate_v1/revision2。真实Search freshness和CK status均报告candidate_v1及各自版本化消费组；完整请求URL、返回字段、授权页、Gold和就绪观察见 [api-ready.json](../artifacts/m4-03c2/api-ready.json)。

ResumeReadSwitch要求API_READY、同一当前boot、确切freeze和无其他未完成操作，再执行真实就绪、权限、marker和对账。API死亡或换boot必须重新Observe；历史记录不是永久许可。最终提交前再观察存活，OPEN、RESUMED和审计在同一MySQL事务写入。unknown commit先读持久选择/阶段/闸门，合法重试不重复提交或恢复。

## 6. 故障矩阵与结果

[RUNTIME] C2专项14个顶层测试，10个实际控制器SIGKILL边界；8项Commit前故障、4项API_READY后故障和2项Shutdown子场景保存原始证据。child helper在父运行中的SKIP由父测试调用真实子进程，不计为PASS。最终通过数量及全部要求映射见 [test-summary.json](../artifacts/m4-03c2/test-summary.json) 和 [fault-matrix.json](../artifacts/m4-03c2/fault-matrix.json)。

最终C2专项原始日志为 [091715-focused/integration.log](../artifacts/m4-03c2/runs/20261115T091715.256745Z-focused/integration.log)，14 PASS、0 FAIL、1个父运行child helper SKIP。45行矩阵映射用户A–V要求及细分故障，所有故障注入只发生在新私有资源。

| 场景 | 预期和实际验证内容 |
| --- | --- |
| 清单、退出、选择提交、API_READY、Resume共10个SIGKILL前/后 | 提交前回滚，提交后可读回，revision只增一次 |
| API_READY后API死亡/新boot | 死亡拒绝Resume，新boot先重新Observe |
| 最终Resume事务前API死亡 | OPEN/RESUMED回滚，FROZEN/API_READY保留，新boot再观察恢复 |
| COMMITTED后错配置/未管理启动 | 真API启动失败，不自动Resume或切回 |
| 8并发意图/重复Commit | 一个PLANNED胜者，同操作多次提交仍revision=2 |
| UUID、缺提交/越界、隔离、内容/源变化、owner epoch | 有效历史proof后实际注入，Commit拒绝，A/revision1及FROZEN不变 |
| ready后ES/CK数据库UUID、缺Offset、非法Kafka隔离 | 历史API_READY不能放行，Resume拒绝 |
| A目标缺失/legacy隔离 | 独立故障场景B数据、版本化状态和Gold仍正确，不用作A健康证明 |
| Resume后HTTP创建/评论 | 11工单49历史，A/B全量和规范统计一致，传播进程身份不变 |

[RUNTIME] 失败尝试全部保留：测试结构字段错误、Handoff主记录漏读分区、故障代理端点接线、活跃消费者覆盖越界注入、清理重复等待退出结果，以及过早构建覆盖另一轮在用二进制。最后一项被身份检查拒绝，运行器新增整轮互斥锁后重新固定执行。一次B回归在私有故障消息发布时context canceled，未到达故障断言；记录并重跑，不猜测归因、不降低检查。失败日志不作通过证据。

## 7. 环境、回归与复现

[RUNTIME] C2实例为MySQL8.4.5/33314、Kafka4.4.0/19106及Controller19107、ES8.17.5/19208、CK/18131。源UUID、集群/目录、cluster ID、配置和PID先核验再建业务资源；库/Topic/索引/CK空间均新随机资源，不复用演示或B/C1资产。CK tracker4GiB，私有RSS及磁盘各8GiB、最低空闲磁盘20GiB。资源采样见每轮原文，没有容量或多BrokerHA宣称。

| 命令 | 验证范围 |
| --- | --- |
| python3 scripts/m403c2_environment.py start | 新实例身份与预算 |
| python3 scripts/m403c2_suite.py focused | C2真实全进程专项 |
| python3 scripts/m403c2_suite.py read-control | C1 12项 |
| python3 scripts/m403c2_suite.py foundations | A 7项 |
| python3 scripts/m403c2_suite.py handoff | B 21项 |
| python3 scripts/m403c2_suite.py regression | MySQL78+syncpipe11项M1–M5相关回归 |
| python3 scripts/m403c2_suite.py static | Go race、go vet、Python测试 |
| python3 scripts/m403c2_preservation.py | 原演示只读内容、身份、进程和位置 |
| python3 scripts/m403c2_environment.py stop | 精确私有PID退出/端口核验，保留数据和证据 |
| python3 scripts/m403c2_package.py | 源码/历史哈希、完整Diff和字节重建，任何验收失败拒绝PASS |

精确起止时间、源码hash、命令、身份、资源采样和日志索引见test-summary。没有新三Broker主题、十万性能、跨主机或Linux/Docker验收。

最终通过的各轮日志如下。时间按UTC记录，每轮保存完整命令、源码SHA-256和资源采样；所有可执行源码及三个环境/运行脚本均与最终工作区一致。

| 验证 | 最终原始日志 | 实际结果 |
| --- | --- | --- |
| C2 | [091715-focused](../artifacts/m4-03c2/runs/20261115T091715.256745Z-focused/integration.log) | 14顶层PASS |
| C1 | [090437-read-control](../artifacts/m4-03c2/runs/20261115T090437.778094Z-read-control/integration.log) | 12顶层PASS |
| M4-03A | [090718-foundations](../artifacts/m4-03c2/runs/20261115T090718.743779Z-foundations/integration.log) | 7顶层PASS |
| M4-03B | [091215-handoff](../artifacts/m4-03c2/runs/20261115T091215.775950Z-handoff/integration.log) | 21顶层PASS |
| M1–M5 | [092505-regression](../artifacts/m4-03c2/runs/20261115T092505.188927Z-regression/integration.log)、[syncpipe](../artifacts/m4-03c2/runs/20261115T092505.188927Z-regression/syncpipe-integration.log) | 78+11顶层PASS |
| Go race | [092750-static/unit-race](../artifacts/m4-03c2/runs/20261115T092750.941494Z-static/unit-race.log) | 7个含测试包PASS；各专项/回归也以-race执行 |
| go vet | [092750-static/vet](../artifacts/m4-03c2/runs/20261115T092750.941494Z-static/vet.log) | exit0 |
| Python | [092750-static/python-tests](../artifacts/m4-03c2/runs/20261115T092750.941494Z-static/python-tests.log) | 3 PASS；本地假服务只验证既有驱动 |

上述最终轮共222个资源样本：最高私有RSS4,337,303,552字节，最高私有目录2,247,529,586字节，最低空闲磁盘242,614,738,944字节，未触及既定停止预算。这是隔离正确性实验的资源观察，不是容量证明。

维护CLI使用原控制DSN和环境中的探针凭据；凭据不放命令参数、不回显：

~~~text
ticketpulse-control -mode=migrate-read-runtime
ticketpulse-control -mode=read-begin -operation=forward_live -version=candidate_v1 -read-revision=1
ticketpulse-control -mode=read-switch-drain-plan -operation=forward_live -api-scope=<private-inventory.json>
ticketpulse-control -mode=read-validate -operation=forward_live
ticketpulse-control -mode=read-switch-drain-observe -operation=forward_live
ticketpulse-control -mode=read-switch-commit -operation=forward_live
ticketpulse-control -mode=read-switch-observe-ready -operation=forward_live -api-boot=<new-boot> -probe-start=2026-11-01 -probe-end=2026-11-04
ticketpulse-control -mode=read-switch-resume -operation=forward_live -api-boot=<new-boot> -freeze-id=<exact-freeze> -probe-start=2026-11-01 -probe-end=2026-11-04
ticketpulse-control -mode=read-switch-status -operation=forward_live
~~~

排空命令保存/观察范围，不自动停进程；操作者按核对的私有PID停止。初始011/012/013登记、冻结、B VERIFIED和合法read-init仍是前提。

## 8. 原环境、历史资产与可重建源码

[RUNTIME] 原演示九张业务/身份表、ES全部字段与external version、CK全部物理行与既有快照比较，原MySQL/Kafka/ES/CK/API继续可访问，011–014未安装。原CDC周期标记自然推进checkpoint和组位置，保留前后数值，不把自然推进当Offset迁移。私有实例停止只向已核对PID发信号，没有pkill或原setup/stop/reset，数据目录和失败证据保留。

私有实例于2026-11-15 09:29:05 UTC完成实际进程退出，7个私有服务端口全部关闭，见 [stop.json](../artifacts/m4-03c2/environment-closeout/20261115T092905.492711Z/stop.json)。私有API/worker和launchd入口也已退出，见 [private-application-closeout.json](../artifacts/m4-03c2/private-application-closeout.json)。停止前后原演示五个PID和启动时间逐项相同，见 [demo-processes-after-private-stop.json](../artifacts/m4-03c2/demo-processes-after-private-stop.json)；原进程均自11月8日运行。

保留证据见 [source-offset-preservation.json](../artifacts/m4-03c2/source-offset-preservation.json)、[demo-progress-preservation.json](../artifacts/m4-03c2/demo-progress-preservation.json)、[previous-evidence-preservation.json](../artifacts/m4-03c2/previous-evidence-preservation.json)。最终归档逐文件读回，full.diff在临时目录从基线应用后比较全部字节，见 [diff-verification.json](../artifacts/m4-03c2/diff-verification.json)。未暂存、提交、推送或部署。

[RUNTIME] 完整Diff的check/apply均exit0，重建后288个文件字节完全一致。7,248个既有证据共3,385,158,726字节逐文件SHA-256核验通过，改变文件数为0；最终index仍为空。7次失败执行及其源码快照和原始日志保留在runs/，没有用于替代最终通过证据。

## 9. 验收结论与C3边界

1. [CODE][RUNTIME] A→B在独立真实全进程环境完成：选择B，API ES/CK与状态来自B，实际读请求和Gold验证后显式恢复业务。
2. [RUNTIME][INFER] 受控范围内无跨版本混读：PLANNED拒绝新读，旧在途/旧二进制必须实际退出才能提交，B新boot只用当前revision。结论限本报告定义的同机范围。
3. [RUNTIME] SIGKILL可按持久阶段恢复，提交未知先查状态；API死亡保持冻结并重新Observe，不提前恢复业务。
4. [RUNTIME] Resume后A/B继续接续HTTP新增业务，完整内容和规范统计一致，源/消费位置不重置。
5. [INFER] PID、可执行、监听及launchd观察为macOS同机受控保证，不等同任意启动隔离、请求级写入fencing、跨主机HA或热切换。
6. [INFER][UNKNOWN] 可进入C3实现，正式逆向尚未实现/验收。C3需审计修订链允许B→A/revision3、旧A同等级新冻结marker/组位置/全量对账、反向排空与API切换、拒绝滞后或过期旧投影，以及A→B→A真实游标验证。不得重置任一组Offset实现回滚。
