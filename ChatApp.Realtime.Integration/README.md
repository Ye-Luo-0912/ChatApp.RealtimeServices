# ChatApp.Realtime.Integration

面向业务服务和长连接网关的独立实时通信模块，目标框架为 .NET 10；
.NET 10 及更高版本项目可以直接引用。

模块封装：

- IRealtimeMessageBus：消息发布、送达/已读回执、历史 request/reply、实时事件发布和网关事件消费。
- RealtimeIntegrationOptions：NATS 地址、Subjects、Streams、消费者与重试参数。
- RealtimeWireSerializer：跨进程消息、回执和历史分页的统一 JSON 契约。
- RealtimeIntegrationTelemetry.CreateIdentityHeaders：网关注入 `X-Chat-User-Id` / `X-Chat-Session-Id`。

事务 Outbox 模型与 EF Core 映射已拆到按需引用的
`ChatApp.Realtime.Outbox.EntityFrameworkCore` 包；该包保留
`ChatApp.Realtime.Integration.Outbox` 命名空间以兼容现有业务源码和 EF migration。
网关与 PushWorker 只引用本 Integration 包，不会再传递引入 EF Core。
由于 public Outbox 类型迁出程序集会影响已编译消费者，移除动作从 Integration 3.0.0
开始生效；Outbox 包采用独立的 1.x 版本谱系。

客户端模块默认 ManageStreams=false，不修改服务端副本数或容量。JetStream 流由
ChatApp.RealtimeServices 创建和校准。入站消息、回执和实时事件使用独立 Stream；
chat.message-history.query 是不持久化的 Core NATS request/reply subject。

TCP 网关：

- 聊天上行调用 PublishIncomingMessageAsync（自动注入发送方身份头）。
- 送达或已读上行调用 PublishMessageReceiptAsync（自动注入接收方身份头）。
- 历史读取调用 QueryMessageHistoryAsync，并由网关注入已认证 UserId（总线会写入身份头）。
- 下行使用每网关实例独立 durable consumer 的 ConsumeEventsAsync。
- Server Saga 等对账方使用 ConsumeAccountCleanupEventsAsync（共享 durable，订阅 AccountCleanupSubject）。
- 不在网关中复制 NATS、数据库或 Outbox 代码。
