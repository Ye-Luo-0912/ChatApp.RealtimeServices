namespace ChatApp.RealtimeServices.Options;

public sealed class RealtimeOptions
{
    public required string ServiceName { get; init; }
    public required string InstanceId { get; init; }
    public int WorkerIntervalMs { get; init; } = 1000;
    public bool EnableDetailedErrors { get; init; }
    public int ProcessingConcurrency { get; init; } = 4;
    public int ProcessingQueueCapacity { get; init; } = 512;

    /// <summary>
    /// Perf-6：入站 Worker 队列的字节预算（跨全部分区共享）。
    /// 单条正文允许 65,536 字符，Envelope 同时持有反序列化 Command 与原始 RawPayload，
    /// 大消息压力下单 Worker 队列可能占用上百 MB。该预算按 payload 字节长度计费，
    /// 超预算时对入队施加背压（等待而非立即入队）。
    /// 0 表示不限制字节，仅按条数限制。
    /// </summary>
    public long ProcessingQueueByteBudget { get; init; } = 64 * 1024 * 1024; // 64 MB

    /// <summary>
    /// P0-6：单条消息 payload 的硬上限。超过该值的合法消息在入队前即被拒绝并进入死信，
    /// 避免单条超大消息占满整个队列字节预算。
    /// 该值同时作为 <see cref="Workers.Reliability.ByteBudget"/> 的 MaxSinglePayloadBytes：
    /// 当单条消息字节数 ≤ 该值但 &gt; <see cref="ProcessingQueueByteBudget"/> 时，
    /// 若队列空闲（无其他占用）则允许独占预算处理。
    /// </summary>
    public int MaxSinglePayloadBytes { get; init; } = 10 * 1024 * 1024; // 10 MB

    /// <summary>
    /// Perf-6：死信 payload 截断上限。死信流不需要保留完整原始 payload，
    /// 仅保留有限长度用于排查。0 表示不截断。
    /// </summary>
    public int DeadLetterPayloadLimitBytes { get; init; } = 4 * 1024; // 4 KB

    /// <summary>
    /// 所有查询类 Worker（历史 / 会话列表 / 已读 / 偏好 / 同步）共享的数据库并发预算。
    /// 旧字段，保留用于兼容。新部署应使用 ReadQueryConcurrency / InteractiveQueryConcurrency / MutationQueryConcurrency。
    /// </summary>
    public int HistoryQueryConcurrency { get; init; } = 8;

    /// <summary>
    /// Perf-6：读类查询 Worker（History / Sync / ConversationList）的数据库并发预算。
    /// 重型读操作可能长时间占用连接，需要较高吞吐但允许排队。
    /// </summary>
    public int ReadQueryConcurrency { get; init; } = 6;

    /// <summary>
    /// Perf-6：交互类查询 Worker（MarkRead / SetPrefs）的数据库并发预算。
    /// 低延迟操作，需要快速响应，不应被重型读饿死。
    /// </summary>
    public int InteractiveQueryConcurrency { get; init; } = 4;

    /// <summary>
    /// Perf-6：变更类查询 Worker（Group / Edit / Recall / Reaction）的数据库并发预算。
    /// 写操作通常中等延迟，需要独立的并发池避免与读操作互相影响。
    /// </summary>
    public int MutationQueryConcurrency { get; init; } = 4;

    /// <summary>每个查询 Worker 的入队容量（非总和）。</summary>
    public int HistoryQueryQueueCapacity { get; init; } = 256;

    /// <summary>
    /// 每个查询 Worker 的通道读取槽位数。实际 DB 并发仍受 <see cref="HistoryQueryConcurrency"/> 限制。
    /// </summary>
    public int HistoryQueryWorkerSlots { get; init; } = 2;
    public int TransientRetryDelayMs { get; init; } = 1000;
    public int PoisonDeliveryThreshold { get; init; } = 8;
    public int ReadinessHeartbeatTimeoutMs { get; init; } = 30_000;

    /// <summary>
    /// 过载协议：入队等待超时（毫秒）。当查询 Worker 的有界通道已满时，
    /// 在该超时内仍无法入队则立即向客户端回复 <c>server_busy</c>，
    /// 而非无限等待到客户端超时。0 表示禁用过载快速失败（退回旧行为）。
    /// </summary>
    public int OverloadEnqueueTimeoutMs { get; init; } = 100;

    /// <summary>
    /// 过载协议：共享并发门等待超时（毫秒）。已入队但无法在超时内获取
    /// <see cref="HistoryQueryConcurrency"/> 信号量时，回复 <c>server_busy</c>。
    /// 0 表示禁用过载快速失败。
    /// </summary>
    public int OverloadGateTimeoutMs { get; init; } = 200;

    /// <summary>
    /// 过载协议：回复给客户端的建议重试间隔（毫秒）。
    /// 客户端应在收到 <c>server_busy</c> 后至少等待该时长再重试。
    /// </summary>
    public int OverloadRetryAfterMs { get; init; } = 500;

    /// <summary>
    /// LongTerm-2：账号清理 Saga 每批处理的附件数量。内存有界，默认 200。
    /// </summary>
    public int AccountCleanupBatchSize { get; init; } = 200;

    /// <summary>
    /// LongTerm-2：账号清理 Saga 单阶段最大重试次数。超过后标记 failed，避免无限重试。
    /// </summary>
    public int AccountCleanupMaxRetries { get; init; } = 5;

    /// <summary>
    /// LongTerm-2：账号清理 Saga 轮询间隔（毫秒）。无 pending 作业时的空转间隔。
    /// </summary>
    public int AccountCleanupPollIntervalMs { get; init; } = 1000;

    /// <summary>
    /// LongTerm-2：账号清理 Saga 单周期最大处理的作业数。防止长时间占用 Worker。
    /// </summary>
    public int AccountCleanupMaxBatchesPerCycle { get; init; } = 10;

    /// <summary>
    /// 六-1：账号清理 Saga 作业租约时长（毫秒）。认领后在此时间内其他实例不会抢占；
    /// 到期后 running 作业可被重新认领。每批处理后会续租。
    /// </summary>
    public int AccountCleanupLeaseMs { get; init; } = 60_000;
}
