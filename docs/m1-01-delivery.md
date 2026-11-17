# M1-01 交付与验收

本轮已在独立工程完成第一条真实业务链路：预置账号创建工单，自动确定受理团队，同一个 MySQL 事务提交工单、创建历史和幂等结果，详情返回正确状态与历史。真实 HTTP 验证得到工单 **TP-000000000001**，提单人 1、初始与当前团队 10、状态待处理、空负责人、version=1、创建历史 1 条。

## 工作区与范围

| 项目 | 实际状态 |
| --- | --- |
| 项目目录 | 本机 ChatGPT 目录下独立的 ticketpulse，具体绝对路径见 artifacts/workspace-result.json |
| 分支 | feat/m1-01-ticket-loop |
| Git | 本地初始化，无提交、无 remote；本轮新增文件未暂存 |
| 业务源 | MySQL Community Server 8.4.5；独立数据目录 .local/mysql-data |
| API | 127.0.0.1:18080；只监听本机 |
| 产品依据 | 本轮用户粘贴的 PRD v0.2（2026-11-03），按本轮 M1-01 限定范围实施 |

没有接入 MQ、ES、CK，也没有实现评论、认领、转派、后续状态动作、完整列表或前端。

角色和团队配置采用本地预置数据，Bearer token 表示本轮测试登录身份，权限仍由服务端数据库执行。没有额外实现账号/会话管理或登录界面。PRD 的标题建议 5–120 字、描述最多 5,000 字作为本轮 API 约束；创建幂等保留合同为持续保留，不自动清理。

## 核心结构与真实调用链

模块职责及完整目录表见 README；API 输入输出见 docs/api.md；DDL、索引与事务设计见 docs/design.md。关键执行顺序如下：

~~~mermaid
sequenceDiagram
    participant C as 已登录本地账号
    participant H as HTTP 接口
    participant S as 工单服务
    participant M as MySQL
    C->>H: POST 创建（request_id + 内容）
    H->>M: 校验 Bearer 所属有效用户
    H->>S: 创建人 ID 与请求
    S->>S: 字段校验、内容 SHA-256、候选团队
    S->>M: Begin READ COMMITTED
    S->>M: 有效用户、请求唯一键占位、有效团队
    S->>M: 工单 + 创建历史 + 请求结果
    S->>M: Commit
    M-->>S: 提交成功
    H-->>C: 201 工单号 / 团队 / version=1
    C->>H: GET 详情
    H->>M: Bearer 身份、只读事务、当前权限
    M-->>H: 同一快照中的工单、团队、负责人、历史
    H-->>C: 200 详情
~~~

创建入口位于 internal/httpapi/api.go 的 create，进入 internal/ticket/service.go 的 Service.Create 后，再调用 internal/mysqlstore/create.go 的 Store.Create。该仓储只执行同库事务，成功返回位置在 Commit 之后。详情入口是 API.detail→Service.Detail→Store.Detail；授权判断与详情数据处于同一个 REPEATABLE READ 只读事务。

创建幂等由数据库唯一约束 (creator_id,request_id) 保证。相同内容返回原工单，不同内容返回冲突，不同用户独立。并发插入在唯一键上竞争，后到请求等待提交后读取胜方结果。操作历史不是异步事件，同事务内插入成功才允许创建完成。

## 业务验收

以下合同全部通过真实 MySQL 测试；完整标准输出见 artifacts/integration.log，执行元信息见 artifacts/integration-result.json。所有集成测试都创建自己的随机新数据库，结束时删除该测试库，不清空演示数据库。

| 用户要求 | 实际证据 |
| --- | --- |
| 1. 正常创建成功 | TestCreateAndDetail；真实 HTTP 首次返回 201 |
| 2. 类别命中正确团队 | TestRouting/network_route、device_route |
| 3. 未命中使用默认团队 | TestRouting/unmapped_default |
| 4. 没有有效团队拒绝创建 | TestRouting/all_teams_inactive、inactive_default、missing_default；三表无新增 |
| 5. 同 request_id 重试返回相同工单 | TestIdempotency；真实 HTTP 重试返回 200 且同 ID |
| 6. 相同请求 ID 内容不同冲突 | TestIdempotency、TestConcurrentDifferentContent；真实 HTTP 409 |
| 7. 并发重复最多一张 | TestConcurrentDuplicateCreate：24 并发、1 创建、23 重放，工单/历史/幂等均为 1 |
| 8. 事务失败不分离 | MySQL trigger 注入历史 INSERT 失败及请求结果 UPDATE 失败；三表均回滚为 0 |
| 9. 详情历史、版本正确 | TestCreateAndDetail、TestStableHistoryOrder；等时历史按版本稳定排序 |
| 10. 无权限详情拒绝 | TestDetailAuthorization、TestHTTPCreationAndAccess；拒绝响应只有 error |

额外合同也通过：成功提交后丢失 HTTP 响应，原 request_id 重试安全返回原工单；不同用户请求 ID 不冲突；请求 ID 大小写敏感；无效/缺失/其他组织的路由团队不能被选中；移出团队、账号停用和角色变化即在新请求中生效；详情按当前团队授权，创建历史的初始团队快照不被改写；最长 Unicode 描述与代理对 JSON 转义在 HTTP、MySQL 和历史中完整保留；所有 PRD 状态值均可被 DDL 存储。

## 验证命令、结果与限制

| 命令 | 结果 |
| --- | --- |
| make test | 单元测试通过，数据库测试不由此命令执行 |
| make integration | 真实 MySQL 8.4.5 集成与 race 检查全部通过；带 integration tag，缺数据库配置会失败而不是假装通过 |
| make vet | Go 静态检查通过 |
| make build | 独立 Go 二进制编译通过 |
| make smoke | 真实运行服务的创建、详情、重试、冲突与权限验证通过 |
| make setup 重跑 | 演示数据保留，未重建数据库 |
| 新空目录发行包解压验证 | 官方发行包校验与 Python 3.9 安装路径通过，MySQL 二进制可运行 |
| 显式状态列迁移 | 演示库状态列扩至 24 字符，原工单、历史和幂等记录保留 |

最终运行库回读为工单 1、历史 1、幂等记录 1。首次 HTTP 创建证据保存在 artifacts/smoke-initial-result.json；最近运行结果在 artifacts/smoke-result.json。未执行 Docker Compose 启动、其他操作系统与 MySQL 版本验证、断电持久化/网络代理故障、性能负载测试、其他 M1 动作；这些不作为本轮已通过的项目。

## 代码量与依赖

本轮 PRD 要求持久返回原结果、检查创建内容冲突并在事务内保存历史。实现使用 Go 标准库和公开 MySQL 驱动；幂等记录与对应工单在同库事务中保存。

新增实现与测试合计 2,055 行：Go 业务代码 834 行、Go 测试 769 行、SQL 151 行、本地 Python 工具 301 行。机器统计见 artifacts/code-size.json，按物理行计算（含空行、注释），排除 .git、.local、二进制、缓存和原始日志，文档与配置另外计数。Go 直接依赖只有 github.com/go-sql-driver/mysql v1.8.1，间接依赖为 filippo.io/edwards25519 v1.1.0；测试使用 Go 标准 testing，无额外测试框架。

## M1-02 入口

下一轮可直接使用预置身份、有效团队与成员模型、当前详情权限、静态受理路由、工单/业务状态快照、历史版本唯一约束、创建幂等，以及隔离 MySQL/race/HTTP 验证脚本。

M1-02 若继续实现认领，入口应在 internal/ticket 增加用例，在 mysqlstore 中检查当前有效 Agent、当前团队、待处理状态、空负责人及期望版本，再同事务更新 assignee_id/version/updated_at 与业务历史。两个同时认领请求应只允许一个成功，失败方返回可重读的版本冲突信息。现有创建合同与历史快照可继续用于回归；不需要引入 RPC。

完整本地登录/退出页面、后续动作级幂等及状态机仍需各自定义接口与测试，不能把本轮创建幂等或仅存储 version 字段误认为已经实现全部 M1 并发合同。
