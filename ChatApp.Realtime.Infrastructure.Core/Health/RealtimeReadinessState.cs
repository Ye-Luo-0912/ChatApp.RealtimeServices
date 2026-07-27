using System.Collections.Concurrent;

namespace ChatApp.Realtime.Infrastructure.Core.Health;

public sealed class RealtimeReadinessState
{
    private readonly ConcurrentDictionary<string, WorkerReadinessSnapshot> _workers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, WorkerKind> _workerKinds = new(StringComparer.Ordinal);

    /// <summary>
    /// Reliability-2：注册必需（关键）Worker 名称。<see cref="GetSnapshot"/> 会验证所有必需 Worker
    /// 都已启动且处于 Running 状态，避免某个 Worker 从未启动时 readiness 仍返回 true。
    /// 关键 Worker 的 SubscriptionConnected / QueueFullSince 等进展字段参与就绪判定。
    /// </summary>
    public void RegisterRequiredWorker(string workerName)
    {
        _workerKinds[workerName] = WorkerKind.Critical;
    }

    /// <summary>
    /// 注册非关键（清理类）Worker。不阻断就绪判定，但状态仍可在快照中观察。
    /// </summary>
    public void RegisterNonCriticalWorker(string workerName)
    {
        _workerKinds[workerName] = WorkerKind.NonCritical;
    }

    /// <summary>查询 Worker 类型，未注册返回 Critical（保守默认）。</summary>
    private WorkerKind GetWorkerKind(string workerName)
        => _workerKinds.TryGetValue(workerName, out var kind) ? kind : WorkerKind.Critical;

    public void MarkStarted(string workerName)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _workers[workerName] = new WorkerReadinessSnapshot(
            workerName,
            RealtimeWorkerStatus.Running,
            now,
            now,
            null,
            LastMessageConsumedAt: null,
            LastOperationSucceededAt: null,
            LastDatabaseSuccessAt: null,
            QueueDepth: null,
            QueueCapacity: null,
            QueueFullSince: null,
            SubscriptionConnected: null);
    }

    public void MarkHeartbeat(string workerName)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _workers.AddOrUpdate(
            workerName,
            static (name, timestamp) => new WorkerReadinessSnapshot(
                name,
                RealtimeWorkerStatus.Running,
                timestamp,
                timestamp,
                null,
                LastMessageConsumedAt: null,
                LastOperationSucceededAt: null,
                LastDatabaseSuccessAt: null,
                QueueDepth: null,
                QueueCapacity: null,
                QueueFullSince: null,
                SubscriptionConnected: null),
            // Reliability-3：心跳只刷新 LastHeartbeatAtMs，不修改 Status 或清除 LastError。
            // Faulted Worker 即使心跳仍在走也不应被误判为健康；恢复需通过 RecordMessageConsumed
            // 或 MarkSubscriptionConnected(true) 显式信号。
            static (_, current, timestamp) => current with
            {
                LastHeartbeatAtMs = timestamp
            },
            now);
    }

    /// <summary>
    /// Reliability-2：记录成功消费一条消息。同时刷新 LastOperationSucceededAt，
    /// 使 readiness 反映真实工作进展而非仅 PeriodicTimer 存活。
    /// </summary>
    public void RecordMessageConsumed(string workerName)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _workers.AddOrUpdate(
            workerName,
            static (name, timestamp) => new WorkerReadinessSnapshot(
                name,
                RealtimeWorkerStatus.Running,
                timestamp,
                timestamp,
                null,
                LastMessageConsumedAt: timestamp,
                LastOperationSucceededAt: timestamp,
                LastDatabaseSuccessAt: null,
                QueueDepth: null,
                QueueCapacity: null,
                QueueFullSince: null,
                SubscriptionConnected: null),
            static (_, current, timestamp) => current with
            {
                // Reliability-3：成功消费表示 Worker 已恢复，显式回到 Running 并清除错误。
                Status = RealtimeWorkerStatus.Running,
                LastHeartbeatAtMs = timestamp,
                LastMessageConsumedAt = timestamp,
                LastOperationSucceededAt = timestamp,
                LastError = null
            },
            now);
    }

    /// <summary>Reliability-2：记录一次成功操作（处理完成 / 查询返回等）。</summary>
    public void RecordOperationSucceeded(string workerName)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _workers.AddOrUpdate(
            workerName,
            static (name, timestamp) => new WorkerReadinessSnapshot(
                name,
                RealtimeWorkerStatus.Running,
                timestamp,
                timestamp,
                null,
                LastMessageConsumedAt: null,
                LastOperationSucceededAt: timestamp,
                LastDatabaseSuccessAt: null,
                QueueDepth: null,
                QueueCapacity: null,
                QueueFullSince: null,
                SubscriptionConnected: null),
            static (_, current, timestamp) => current with
            {
                LastOperationSucceededAt = timestamp
            },
            now);
    }

    /// <summary>Reliability-2：记录一次成功的数据库操作。</summary>
    public void RecordDatabaseSuccess(string workerName)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _workers.AddOrUpdate(
            workerName,
            static (name, timestamp) => new WorkerReadinessSnapshot(
                name,
                RealtimeWorkerStatus.Running,
                timestamp,
                timestamp,
                null,
                LastMessageConsumedAt: null,
                LastOperationSucceededAt: timestamp,
                LastDatabaseSuccessAt: timestamp,
                QueueDepth: null,
                QueueCapacity: null,
                QueueFullSince: null,
                SubscriptionConnected: null),
            static (_, current, timestamp) => current with
            {
                LastDatabaseSuccessAt = timestamp,
                LastOperationSucceededAt = timestamp
            },
            now);
    }

    /// <summary>
    /// Reliability-2：记录队列深度。队列满时设置 QueueFullSince，
    /// 队列恢复时清除。<see cref="GetSnapshot"/> 会在 QueueFullSince 超过心跳超时时判定不就绪。
    /// </summary>
    public void RecordQueueDepth(string workerName, int depth, int capacity)
    {
        _workers.AddOrUpdate(
            workerName,
            static (name, arg) => new WorkerReadinessSnapshot(
                name,
                RealtimeWorkerStatus.Running,
                arg.Now,
                arg.Now,
                null,
                LastMessageConsumedAt: null,
                LastOperationSucceededAt: null,
                LastDatabaseSuccessAt: null,
                QueueDepth: arg.Depth,
                QueueCapacity: arg.Capacity,
                QueueFullSince: arg.Depth >= arg.Capacity ? arg.Now : null,
                SubscriptionConnected: null),
            static (_, current, arg) =>
            {
                var wasFull = current.QueueDepth is int d && current.QueueCapacity is int c && d >= c;
                var isFull = arg.Depth >= arg.Capacity;
                return current with
                {
                    QueueDepth = arg.Depth,
                    QueueCapacity = arg.Capacity,
                    QueueFullSince = isFull
                        ? (current.QueueFullSince ?? arg.Now)
                        : null
                };
            },
            (Now: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), Depth: depth, Capacity: capacity));
    }

    /// <summary>Reliability-2：标记订阅连接状态。</summary>
    public void MarkSubscriptionConnected(string workerName, bool connected)
    {
        _workers.AddOrUpdate(
            workerName,
            static (name, arg) => new WorkerReadinessSnapshot(
                name,
                RealtimeWorkerStatus.Running,
                arg.Now,
                arg.Now,
                null,
                LastMessageConsumedAt: null,
                LastOperationSucceededAt: null,
                LastDatabaseSuccessAt: null,
                QueueDepth: null,
                QueueCapacity: null,
                QueueFullSince: null,
                SubscriptionConnected: arg.Connected),
            static (_, current, arg) => current with
            {
                // Reliability-3：订阅恢复连接时回到 Running 并清除错误。
                Status = arg.Connected ? RealtimeWorkerStatus.Running : current.Status,
                SubscriptionConnected = arg.Connected,
                LastError = arg.Connected ? null : current.LastError,
                LastHeartbeatAtMs = arg.Now
            },
            (Now: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), Connected: connected));
    }

    public void MarkStopped(string workerName)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _workers.AddOrUpdate(
            workerName,
            static (name, timestamp) => new WorkerReadinessSnapshot(
                name,
                RealtimeWorkerStatus.Stopped,
                null,
                timestamp,
                null,
                LastMessageConsumedAt: null,
                LastOperationSucceededAt: null,
                LastDatabaseSuccessAt: null,
                QueueDepth: null,
                QueueCapacity: null,
                QueueFullSince: null,
                SubscriptionConnected: null),
            static (_, current, timestamp) => current with
            {
                Status = RealtimeWorkerStatus.Stopped,
                LastHeartbeatAtMs = timestamp
            },
            now);
    }

    public void MarkFaulted(string workerName, Exception ex)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        _workers.AddOrUpdate(
            workerName,
            static (name, state) => new WorkerReadinessSnapshot(
                name,
                RealtimeWorkerStatus.Faulted,
                null,
                state.Timestamp,
                state.Error,
                LastMessageConsumedAt: null,
                LastOperationSucceededAt: null,
                LastDatabaseSuccessAt: null,
                QueueDepth: null,
                QueueCapacity: null,
                QueueFullSince: null,
                SubscriptionConnected: null),
            static (_, current, state) => current with
            {
                Status = RealtimeWorkerStatus.Faulted,
                LastHeartbeatAtMs = state.Timestamp,
                LastError = state.Error
            },
            (Timestamp: now, Error: ex.Message));
    }

    public RealtimeReadinessSnapshot GetSnapshot(TimeSpan? heartbeatTimeout = null)
    {
        var workers = _workers.Values
            .OrderBy(static worker => worker.Name, StringComparer.Ordinal)
            .ToArray();

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var timeoutMs = heartbeatTimeout is null ? long.MaxValue : (long)heartbeatTimeout.Value.TotalMilliseconds;

        // Reliability-3：按 Worker 类型分别判定。
        // 关键 Worker（RegisterRequiredWorker）必须 Running + 心跳未超时 + 订阅已连接 + 队列未卡死。
        // 非关键 Worker（RegisterNonCriticalWorker，如清理类）不阻断就绪，但状态仍上报。
        var criticalNames = _workerKinds
            .Where(static kvp => kvp.Value == WorkerKind.Critical)
            .Select(static kvp => kvp.Key)
            .OrderBy(static n => n, StringComparer.Ordinal)
            .ToArray();

        var allCriticalRunning = criticalNames.Length == 0
            || criticalNames.All(name =>
            {
                if (!_workers.TryGetValue(name, out var w))
                    return false;
                return IsWorkerHealthy(w, now, timeoutMs);
            });

        // 关键 Worker 中已登记到 _workers 的也参与 allWorkersHealthy（双重保险，
        // 防止关键 Worker 未通过 RegisterRequiredWorker 注册但已 MarkStarted）。
        var allWorkersHealthy = workers.Length == 0 || workers
            .Where(w => GetWorkerKind(w.Name) == WorkerKind.Critical)
            .All(w => IsWorkerHealthy(w, now, timeoutMs));

        return new RealtimeReadinessSnapshot(
            allCriticalRunning && allWorkersHealthy,
            workers,
            criticalNames,
            now);
    }

    /// <summary>
    /// Reliability-3：统一的关键 Worker 健康判定。
    /// 检查项：Status=Running、心跳未超时、订阅已连接（显式 false 则不健康）、队列未长时间满。
    /// </summary>
    private static bool IsWorkerHealthy(WorkerReadinessSnapshot worker, long now, long timeoutMs)
    {
        if (worker.Status != RealtimeWorkerStatus.Running)
            return false;
        if (worker.LastHeartbeatAtMs is not null && now - worker.LastHeartbeatAtMs > timeoutMs)
            return false;
        // 订阅显式断开（false）→ 不健康。null 表示未使用订阅语义（如查询 Worker），不阻断。
        if (worker.SubscriptionConnected == false)
            return false;
        // 队列持续满超过超时阈值 → 处理卡死，不就绪。
        if (worker.QueueFullSince is long fullSince && now - fullSince > timeoutMs)
            return false;
        return true;
    }
}

public sealed record RealtimeReadinessSnapshot(
    bool IsReady,
    IReadOnlyCollection<WorkerReadinessSnapshot> Workers,
    IReadOnlyList<string> RequiredWorkerNames,
    long GeneratedAtMs);

public sealed record WorkerReadinessSnapshot(
    string Name,
    RealtimeWorkerStatus Status,
    long? StartedAtMs,
    long? LastHeartbeatAtMs,
    string? LastError,
    long? LastMessageConsumedAt,
    long? LastOperationSucceededAt,
    long? LastDatabaseSuccessAt,
    int? QueueDepth,
    int? QueueCapacity,
    long? QueueFullSince,
    bool? SubscriptionConnected);

public enum RealtimeWorkerStatus
{
    Unknown = 0,
    Running = 1,
    Stopped = 2,
    Faulted = 3
}

/// <summary>
/// Reliability-3：Worker 类型决定是否参与就绪判定。
/// Critical = 关键 Worker（消息/事件/查询处理），阻断 readiness。
/// NonCritical = 清理类 Worker（AccountCleanup/OutboxCleanup/MessageRetention），不阻断。
/// </summary>
public enum WorkerKind
{
    Critical = 0,
    NonCritical = 1
}
