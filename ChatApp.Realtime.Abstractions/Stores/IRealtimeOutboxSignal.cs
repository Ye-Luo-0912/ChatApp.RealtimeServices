namespace ChatApp.Realtime.Abstractions.Stores;

public interface IRealtimeOutboxSignal
{
    void Notify();

    /// <summary>
    /// 通知发布器某个 Outbox 事件已经随业务事务提交。实现可以把事件编号作为
    /// 非权威的快速认领提示；不支持提示的实现仍可退化为普通唤醒。
    /// </summary>
    void Notify(string committedEventId) => Notify();

    ValueTask<bool> WaitAsync(
        TimeSpan timeout,
        CancellationToken ct = default);
}

/// <summary>
/// Outbox 提交提示的单消费者读取端。提示仅用于绕过全局 Pending 索引扫描；
/// 数据库 Outbox、claim token 和租约仍是可靠性权威，丢失提示必须可由恢复扫描补偿。
/// </summary>
public interface IRealtimeOutboxHintSource
{
    bool TryReadCommittedHint(out RealtimeOutboxHint hint);

    bool TryReadCommittedEventId(out string eventId)
    {
        if (TryReadCommittedHint(out var hint))
        {
            eventId = hint.EventId;
            return true;
        }

        eventId = string.Empty;
        return false;
    }
}

/// <summary>
/// 提交后的非权威发布提示。带 owner/token 时表示 Outbox 行已在业务事务内预领取，
/// Publisher 只需校验并读取；不带 token 时继续走精确认领 UPDATE。
/// </summary>
public readonly record struct RealtimeOutboxHint(
    string EventId,
    string? LockOwner = null,
    string? ClaimToken = null)
{
    public bool IsPreclaimed =>
        !string.IsNullOrWhiteSpace(LockOwner)
        && !string.IsNullOrWhiteSpace(ClaimToken);
}

/// <summary>业务事务内预写入 Outbox 的租约凭据。</summary>
public readonly record struct RealtimeOutboxPreclaim(
    string EventId,
    string LockOwner,
    string ClaimToken,
    long LockedUntilMs);

/// <summary>
/// 共享信号的可选预领取协调能力。预留发生在 INSERT 前，只有事务提交后才把凭据发布给
/// consumer；回滚/冲突必须取消预留。提示丢失时数据库租约到期后仍由恢复扫描接管。
/// </summary>
public interface IRealtimeOutboxPreclaimCoordinator
{
    void ConfigurePreclaimOwner(string instanceId, TimeSpan leaseDuration);

    bool TryReservePreclaim(string eventId, out RealtimeOutboxPreclaim preclaim);

    void CommitPreclaim(RealtimeOutboxPreclaim preclaim);

    void CancelPreclaim(RealtimeOutboxPreclaim preclaim);
}
