# TicketPulse 实现与依赖说明

本文说明当前项目的职责分层、事务边界与公开依赖。业务协议和数据模型分别见 [API 合同](api.md) 与 [事务设计](design.md)。

HTTP 层负责认证、参数及错误合同；工单服务负责业务规则；MySQL Store 负责事务、当前权限和持久化。工单状态、版本、评论及对应历史在同一事务中提交。创建意图用用户与请求标识的唯一记录、内容摘要和原结果实现幂等。

搜索及统计是异步投影：已提交 Binlog 经 CDC 发布到 Kafka，两个独立消费组分别写入 ES 和 CK。搜索候选返回前复核当前 MySQL 权限与业务版本，详情和基础列表读取 MySQL；CK 查询按业务键去重，并按最大版本选择当前状态。

| 组件 | 当前使用 |
| --- | --- |
| Go 标准库 | HTTP、SQL、JSON、上下文取消、日志及密码学工具 |
| MySQL 驱动和 Binlog 客户端 | github.com/go-sql-driver/mysql、github.com/go-mysql-org/go-mysql |
| Kafka 客户端 | github.com/twmb/franz-go 及 kadm 管理接口 |
| 密码处理 | golang.org/x/crypto 的 bcrypt |
| 浏览器验证 | Playwright；产品运行无需 Node |

依赖版本以 go.mod、go.sum 与 package-lock.json 为准。同步器包含显式错误分类和有限指数退避：仅对允许重试的错误执行下一次尝试，取消和超时按上下文结束；成功或持久隔离后再保存进度及提交消费位置。

投影重建、消费交接和受控读切换属于本项目的持久合同。适用范围与真实验收边界见各轮交付及 [演示手册](demo-runbook.md)。
