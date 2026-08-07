namespace ChatApp.Realtime.Abstractions.Messaging;

/// <summary>
/// LongTerm-1：独立命令幂等性账本保留配置。
/// <para>
/// JetStream durable 在重建后会以 <c>DeliverPolicy.All</c> 回放旧命令。若消息行已被
/// retention GC 或账号删除清理，则幂等依据（messages 唯一索引）随之消失，旧命令会被
/// 当作新消息重新写入（"复活"）。独立幂等账本解耦此依赖，其保留期必须不少于
/// JetStream 最大回放周期（<c>Nats:JetStream:MaxAgeHours</c>）。
/// </para>
/// <para>
/// 启动时由宿主的 <c>RealtimeStartupReporter</c> 校验
/// <c>ResolveEffectiveHorizonMs(jetStreamMaxAgeMs) &gt;= jetStreamMaxAgeMs</c>。
/// </para>
/// </summary>
public sealed class IdempotencyOptions
{
    public const string SectionName = "Idempotency";

    /// <summary>
    /// 主开关。关闭时 GC Worker 不运行；但账本写入仍会发生（只要存储已注册）。
    /// 默认 true，因为账本保留期必须覆盖 JetStream 回放窗口，否则旧命令可能"复活"。
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// 显式保留窗口（毫秒）。0 时回退到 <see cref="RetentionDays"/>，
    /// 再回退到 JetStream MaxAge（由调用方传入）。
    /// </summary>
    public long RetentionHorizonMs { get; init; }

    /// <summary>
    /// 保留天数。默认 8 天（略高于 JetStream 默认 MaxAgeHours=168h=7d），
    /// 为 GC 周期留出 1 天余量。
    /// </summary>
    public int RetentionDays { get; init; } = 8;

    public int BatchSize { get; init; } = 500;

    /// <summary>GC Worker 轮询间隔。</summary>
    public int IntervalMs { get; init; } = 60_000;

    /// <summary>批次间休眠（在线安全限速）。</summary>
    public int BatchSleepMs { get; init; } = 100;

    /// <summary>每轮最大删除批次（0 = 不限，直到某批返回 0）。</summary>
    public int MaxBatchesPerCycle { get; init; } = 100;

    /// <summary>
    /// 解析有效保留窗口：显式 ms → RetentionDays → jetStreamMaxAgeMs。
    /// </summary>
    public long ResolveEffectiveHorizonMs(long jetStreamMaxAgeMs)
    {
        if (RetentionHorizonMs > 0)
            return RetentionHorizonMs;
        if (RetentionDays > 0)
            return RetentionDays * 86_400_000L;
        return jetStreamMaxAgeMs;
    }
}
