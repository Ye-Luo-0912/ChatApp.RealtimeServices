# ChatApp RealtimeServices

实时消息链路默认使用 JetStream + PostgreSQL Transactional Outbox：入站消息和待发布事件在同一数据库事务提交，Outbox Worker 获得 JetStream 服务端确认后才标记事件完成。

## 可靠性语义

- 入站消息与回执：独立 Subject、JetStream 显式 ACK、延迟重试和有界 MaxAckPending。
- 幂等键：`(sender_user_id, client_message_id)`；事件 ID 由该逻辑键确定性生成。
- 消息、回执状态和对应事件：均与 realtime.outbox 在同一 PostgreSQL 事务提交。
- Outbox：本实例事务提交后以容量 1 的合并信号立即唤醒发布器；多副本仍使用 `FOR UPDATE SKIP LOCKED` 和租约抢占，200 ms 轮询负责跨实例、竞态与恢复兜底；发布失败指数退避。
- 永久错误和坏 JSON：写入 `DEAD_LETTERS` 流后终止原消息，不直接丢弃。
- 实例内吞吐：有界 Channel 分区并发；同一对用户固定到同一分区以维持点对点顺序。
- 历史读取：Core NATS request/reply + 8 路有界工作器；不进入 JetStream 写入流。
- 历史分页：收件人/发件人复合索引 + (ReceivedAtMs, MessageId) keyset cursor，
  默认 50、最多 100 条并限制响应字节。
- 状态：配置 Garnet/Redis 后，IRealtimeStateStore 自动替换为共享状态存储并支持 TTL。

数据库访问保留两种 Provider：普通模型和事务可使用 EF Core；默认高频消息写入与 Outbox 抢占使用 Npgsql 原生 SQL。

本服务负责把已持久化消息可靠发布到 `chat.realtime-events`。仓库目前没有真实认证适配器和 WebSocket/SignalR 网关，因此不会默认启动那个只记录日志的事件消费者，也不会提前 ACK 并丢弃待投递事件；终端推送服务应使用自己的 durable consumer，在实际推送成功后 ACK。

## 与业务 API、TCP 网关的边界

跨项目通信使用版本化的 .NET 10 包：BCL-only 契约位于
`ChatApp.Realtime.Contracts`，NATS/JetStream 客户端位于
[ChatApp.Realtime.Integration](./ChatApp.Realtime.Integration/README.md)，仅业务 API 需要的
EF Outbox 模型与映射位于 `ChatApp.Realtime.Outbox.EntityFrameworkCore`：

1. `ChatApp.Server` 的好友请求、好友列表和屏蔽列表变更，由 EF Core SaveChanges 拦截器在业务事务内写入 `realtime.outbox`。
2. 本服务抢占 Outbox，获得 JetStream 服务端确认后标记发布完成。
3. TCP 网关发布聊天命令与送达/已读回执；本服务持久化后统一发布 REALTIME_EVENTS。
4. 每个网关实例使用独立 durable consumer，避免普通队列组把用户事件分配给没有该用户连接的网关。
5. TCP 网关通过 chat.message-history.query 请求历史；服务只信任网关注入的已认证 UserId。

这些包以 .NET 10 提供；TCP 网关只引用 Contracts/Integration，不会传递引入 EF Core，
也不复制消息存储或 NATS 实现。

## 本地启动

设置数据库密码并启动 PostgreSQL、Garnet 和**单节点** NATS（本地默认；容器名 `chatapp_nats`）：

```powershell
$env:POSTGRES_PASSWORD = "your-local-password"
docker compose `
  -f ..\ChatApp.Server\docker-compose.yaml `
  -f .\docker-compose.nats.yaml `
  up -d postgres_db garnet_cache nats
```

可选三节点 JetStream 集群（HA / `Replicas=3`）：

```powershell
docker compose `
  -f ..\ChatApp.Server\docker-compose.yaml `
  -f .\docker-compose.nats.yaml `
  -f .\docker-compose.nats.cluster.yaml `
  --profile nats-cluster `
  up -d nats nats2 nats3
```

切换单节点 ↔ 集群时请先清掉对应 NATS 数据卷，避免 JetStream 元数据冲突。

本机运行时使用 Development 环境，JetStream 副本数默认是 1，方便同时兼容单节点开发（需 .NET SDK 11 preview，见 `global.json`）：

```powershell
$env:DOTNET_ENVIRONMENT = "Development"
$env:ASPNETCORE_URLS = "http://127.0.0.1:8080"
dotnet run --project .\ChatApp.RealtimeServices\ChatApp.RealtimeServices.csproj -c Release --no-launch-profile
```

连接字符串继续放在 `~/.chatapp/realtime.user.json`，不要提交到仓库：

```json
{
  "ConnectionStrings": {
    "RealtimeDatabase": "Host=localhost;Port=5432;Database=ChatAppDatabase;Username=postgres;Password=...;Pooling=true;Maximum Pool Size=100;Timeout=5;Command Timeout=5",
    "Garnet": "127.0.0.1:6379,abortConnect=false"
  },
  "Nats": {
    "Url": "nats://localhost:4222",
    "Mode": "JetStream"
  }
}
```

开发模式允许 `InitializeSchemaOnStart=true`（`appsettings.Development.json`；走版本化 `schema_migrations` + `RealtimeSchemaMigrationRunner`）。容器/生产环境必须关闭运行时迁移（`appsettings.json` 默认 `false`，非 Development 启动校验会拒绝 `true`），改由独立迁移 Job 执行 runner（009/010 支持检查点续跑与 `CREATE INDEX CONCURRENTLY`）。

P0-1：C# migrations 是唯一事实来源。容器 Compose 的 `realtime_migrations` 服务直接运行 `dotnet ChatApp.RealtimeServices.dll --migrate`，该命令读取 `ConnectionStrings__RealtimeDatabase` 和 `RealtimeDatabase__Schema`，执行全部 23 个版本化迁移后退出。不再维护手写 SQL 脚本。

## 健康与指标

- `GET http://localhost:8080/live`：进程存活。
- `GET http://localhost:8080/ready`：Worker 心跳、NATS、PostgreSQL、Garnet 均健康才返回 200。
- `GET http://localhost:8080/metrics`：Prometheus 文本端点，包含处理计数、历史查询、
  DLQ、Outbox、.NET 运行时、ASP.NET/Kestrel 和 Npgsql Meter。
- `GET http://localhost:8080/diagnostics/runtime`：供开发排查使用的 JSON 运行快照；
  不再与 Prometheus `/metrics` 混用。
- `realtime.outbox.pending`、`realtime.outbox.oldest.age` 和
  `realtime.outbox.max_attempts`：由独立低频采集器更新，不进入消息处理热路径。
- `realtime.history.queue.depth` 和 `realtime.history.in_flight`：历史查询的排队与
  执行中数量，可用于识别工作器已饱和还是数据库查询本身变慢。

`Observability` 配置控制 Prometheus 缓存、Outbox 采样间隔、OTLP 地址和 Trace 采样率。
生产环境建议启用 OTLP，由 Collector 统一转发指标与 Trace；跨 NATS/Outbox 使用 W3C
Trace Context 保持 Gateway 到本服务的关联。

生产编排应分别把 liveness 和 readiness 指向 `/live`、`/ready`。Dockerfile 已包含同等健康检查和 30 秒优雅停机窗口。持久消息、回执、Outbox 和历史查询的可重复负载工具位于同级 TCP 网关仓库的 `tools/ChatApp.Realtime.PipelineLoadGenerator`；正式基准方法见该仓库的 `docs/performance-baseline.md`。

## 验证消息、Outbox 与死信

```powershell
nats pub --header "Nats-Msg-Id:cmd-1" chat.incoming-messages '{"CommandId":"cmd-1","ClientMessageId":"client-1","SenderUserId":1001,"SenderSessionId":"s-1","ReceiverUserId":1002,"Content":"hello","ReceivedAtMs":1719000000000}'

docker exec chatapp_postgres psql -U postgres -d ChatAppDatabase -c "select message_id, client_message_id from realtime.messages order by created_at_ms desc limit 5;"
docker exec chatapp_postgres psql -U postgres -d ChatAppDatabase -c "select event_id, attempt_count, published_at_ms, last_error from realtime.outbox order by created_at_ms desc limit 5;"

nats stream info INCOMING_MESSAGES
nats stream info REALTIME_EVENTS
nats stream view DEAD_LETTERS
```

`scripts/test-jetstream-incoming.ps1` 可验证正常消息、重复消息与永久失败消息。永久失败现在应在第一次处理时进入死信流，不再等待第八次后直接 ACK 丢弃。

## 容器化高可用运行

单节点本地 Compose 默认可跑 `--profile container-service`（Development + `Replicas=1`）。
三节点 HA 需叠加集群文件（Container + `Replicas=3`）、持久卷、数据库迁移任务和两个实时服务副本：

```powershell
$env:POSTGRES_PASSWORD = "replace-me"
docker compose `
  -f ..\ChatApp.Server\docker-compose.yaml `
  -f .\docker-compose.nats.yaml `
  -f .\docker-compose.nats.cluster.yaml `
  --profile nats-cluster `
  --profile container-service `
  up -d --build
```

生产环境仍应使用跨故障域的 NATS/PostgreSQL/Redis 集群、Secret 管理、TLS、PodDisruptionBudget 和反亲和，而不是把单机 Compose 当作最终生产拓扑。

## 构建与测试

```powershell
dotnet restore .\ChatApp.RealtimeServices.slnx
dotnet build .\ChatApp.RealtimeServices.slnx --no-restore --configuration Release
dotnet test .\ChatApp.RealtimeServices.slnx --no-build --configuration Release
docker compose -f ..\ChatApp.Server\docker-compose.yaml -f .\docker-compose.nats.yaml config --quiet
```

CI 同时执行 Release 构建、自动化测试和容器镜像构建。

## 短管道冒烟（可选）

Realtime `/ready` 后，在同级 TCP 网关仓库用 `PipelineLoadGenerator` 跑 ≤1 分钟冒烟（正式 30m 基线见该仓库 `docs/performance-baseline.md`）：

```powershell
cd ..\ChatAppTCP_Server
dotnet run --project .\tools\ChatApp.Realtime.PipelineLoadGenerator -c Release -- `
  --nats-url nats://127.0.0.1:4222 `
  --warmup-seconds 5 --duration-seconds 30 `
  --concurrency 4 --operations-per-second 4 --payload-bytes 512
```
