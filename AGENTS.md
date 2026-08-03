# AGENTS.md — Agent 开发指南

本文件为在此仓库中工作的 Agent（包括子代理）提供开发约定与约束。所有代码改动都应遵循下述开发原则。

## 开发原则（Development Principles）

本项目的开发遵循三条核心原则，按优先级排序。任何代码改动、重构、评审都必须以此为准绳：

### 1. 代码复用（Code Reuse）

- **优先复用现有抽象**：动手写新代码前，先搜索仓库中已有的接口、基类、扩展方法、工具类（如 `ChatApp.Realtime.Abstractions` 中的契约、`Infrastructure.*` 中的实现），避免重复造轮子。
- **抽象下沉到公共层**：跨项目共享的契约和逻辑放在 `Abstractions` / `Infrastructure.Core` 等公共项目中，而不是散落在各个消费方。
- **DRY（Don't Repeat Yourself）**：发现相似逻辑出现两次以上时，提取为共享组件；禁止复制粘贴式编码。
- **复用而非继承滥用**：优先组合（composition）与接口，仅在语义明确时使用继承。

### 2. 高性能（High Performance）

- **实时系统心智**：本项目是实时通信服务（NATS/JetStream/Redis/Postgres 栈），延迟与吞吐是核心指标，写代码时始终考虑热路径（hot path）开销。
- **避免不必要的分配**：热路径中避免 LINQ 滥用、避免字符串拼接、避免装箱/拆箱，优先使用结构体、`ArrayPool`、`ValueTask` 等高性能原语。
- **异步与并发正确性**：使用 `async/await` 时避免同步阻塞（如 `.Result`/`.Wait()`）；注意锁粒度、避免死锁与资源泄漏。
- **I/O 与批量操作**：数据库与消息队列操作尽量批量化；避免 N+1 查询；善用索引与现有缓存（如 Redis/Garnet）。
- **可观测性优先**：性能改动应配合指标（metrics）、追踪（tracing）与基准（`ChatApp.Realtime.Benchmarks`、`benchmark-baselines`），用数据验证改进，不做无依据的"优化"。

### 3. 高可维护（High Maintainability）

- **清晰命名**：类、方法、变量命名自解释，遵循现有代码风格（参考 `Directory.Build.props` 与现有项目约定）。
- **小而专注**：类与方法职责单一；方法过长或职责混杂时应拆分。
- **可测试性**：新逻辑必须可单元测试（参考 `ChatApp.Realtime.Tests` 与 `ChatApp.Realtime.IntegrationTests` 的既有测试模式），关键路径补测试。
- **契约与文档**：公开 API 保持向后兼容；影响面较大的行为变更需更新 `docs/` 下对应文档。
- **不留技术债**：改动时顺手清理碰到的死代码、重复代码与过时注释；不引入新的警告或已禁止的模式。

---

## 提交前自检清单（Checklist）

- [ ] 是否复用了现有抽象，而不是新写一套？
- [ ] 是否避免了热路径上的不必要分配与阻塞？
- [ ] 是否有测试覆盖关键路径？现有测试是否全部通过？
- [ ] 命名与风格是否与现有代码一致？
- [ ] 是否更新了受影响的 `docs/` 文档？
