# RealtimeServices 本地开发运行

当前阶段先不把 `RealtimeServices` 放进容器，也暂时不启用 NativeAOT。NATS、PostgreSQL、Garnet 交给 Docker Compose 管理，实时服务直接在本机用 `dotnet run` 启动。

## 启动基础设施

```powershell
docker compose `
  -f ..\ChatApp.Server\docker-compose.yaml `
  -f docker-compose.nats.yaml `
  up -d postgres_db garnet_cache nats
```

这样会启动：

- `postgres_db`：现有 PostgreSQL
- `garnet_cache`：现有 Garnet
- `nats`：NATS 消息队列，已开启 JetStream 以便后续升级

宿主机连接配置：

- NATS：`nats://localhost:4222`
- NATS 监控：`http://localhost:8222`
- Garnet：`127.0.0.1:6379`
- PostgreSQL：`localhost:5432`

## 启动实时服务

```powershell
dotnet run --project .\ChatApp.RealtimeServices\ChatApp.RealtimeServices.csproj --no-launch-profile
```

当前 `appsettings.json` 默认启用：

- Core NATS 真实消费者/生产者
- Npgsql 直连消息存储
- 启动时初始化 `realtime.messages` 最小表结构

说明：当前使用 Core NATS 做轻量实时链路验证，不提供持久化确认和消息重放。后续需要可靠投递、失败重试、死信和回放时，再在现有 subject 边界上切换到 JetStream。

## 发送测试消息（Core NATS）

```powershell
$ts = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$json = "{`"CommandId`":`"cmd-local-$ts`",`"ClientMessageId`":`"client-local-$ts`",`"SenderUserId`":1001,`"SenderSessionId`":`"session-local-1001`",`"ReceiverUserId`":1002,`"Content`":`"本地开发链路测试消息`",`"ReceivedAtMs`":$ts}"

$client = [System.Net.Sockets.TcpClient]::new("127.0.0.1", 4222)
$stream = $client.GetStream()
$bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
$command = [System.Text.Encoding]::ASCII.GetBytes("PUB chat.incoming-messages $($bytes.Length)`r`n")
$end = [System.Text.Encoding]::ASCII.GetBytes("`r`n")
$stream.Write($command, 0, $command.Length)
$stream.Write($bytes, 0, $bytes.Length)
$stream.Write($end, 0, $end.Length)
$stream.Flush()
$client.Dispose()
```

验证落库：

```powershell
docker exec chatapp_postgres psql -U postgres -d ChatAppDatabase -c "select message_id, client_message_id, sender_user_id, receiver_user_id, content from realtime.messages order by created_at_ms desc limit 5;"
```

## JetStream 模式

### 切换配置

将 `appsettings.json` 中 `Nats:Mode` 改为 `"JetStream"`：

```json
"Nats": {
    "Url": "nats://localhost:4222",
    "Mode": "JetStream",
    ...
}
```

或用户级配置覆盖（`~/.chatapp/realtime.user.json`）：

```json
{
  "ConnectionStrings": { ... },
  "Nats": { "Mode": "JetStream" }
}
```

### JetStream 行为

- 启动时自动创建 `INCOMING_MESSAGES` 流和持久消费者
- 流配置：File 存储、Limits 保留、2 分钟去重窗口
- 消费者：Explicit Ack、30s Ack Wait、最多重投 10 次
- **Ack/Nak 由 Worker 控制**：处理成功 Ack，失败 Nak
- **毒丸策略**：投递次数 ≥ 8 时直接 Ack 丢弃，避免无限重试
- RealtimeEvents 仍走 Core NATS（发布/订阅），不受 JetStream 影响

### 发送 JetStream 测试消息

JetStream 接收的消息格式与 Core NATS 相同，使用 `nats` CLI 发布：

```powershell
nats pub chat.incoming-messages '{
  "CommandId": "cmd-js-1",
  "ClientMessageId": "client-js-1",
  "SenderUserId": 1001,
  "SenderSessionId": "session-js-1",
  "ReceiverUserId": 1002,
  "Content": "JetStream 测试消息",
  "ReceivedAtMs": 1719000000000
}'
```

### 验证 JetStream 状态

```powershell
# 查看流
nats str list

# 查看 INCOMING_MESSAGES 流详情
nats str info INCOMING_MESSAGES

# 查看消费者
nats con ls INCOMING_MESSAGES

# 消费者状态（确认/未确认数）
nats con info INCOMING_MESSAGES chatapp-realtime-services
```

### 验证毒丸处理

发送一条必然失败的消息（例如 `ReceiverUserId` 为 0 触发 Npgsql 约束）：

```powershell
for ($i = 1; $i -le 12; $i++) {
  Write-Host "发送第 $i 条毒丸..."
  nats pub chat.incoming-messages "{\"CommandId\":\"poison-$i\",\"ClientMessageId\":\"poison-$i\",\"SenderUserId\":1001,\"SenderSessionId\":\"s\",\"ReceiverUserId\":0,\"Content\":\"x\",\"ReceivedAtMs\":1}"
  Start-Sleep -Seconds 2
}
```

观察日志输出 — 前 8 次投递会 Nak，第 9 次起 Worker 会输出 `检测到毒丸消息，直接丢弃` 并 Ack 终止。

`realtime_services` 容器服务已经放入 `container-service` profile，不会随默认 compose 命令启动。后续准备做容器部署时再启用该 profile，并重新评估 NativeAOT、基础镜像和 NATS JetStream 客户端兼容性。
