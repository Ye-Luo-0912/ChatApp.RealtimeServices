using System.Collections.Concurrent;

namespace ChatApp.Realtime.Infrastructure.Core.Health;

public sealed class RealtimeReadinessState
{
    private readonly ConcurrentDictionary<string, WorkerReadinessSnapshot> _workers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> _requiredWorkers = new(StringComparer.Ordinal);

    /// <summary>
    /// Reliability-2：注册必需 Worker 名称。<see cref="GetSnapshot"/> 会验证所有必需 Worker
    /// 都已启动且处于 Running 状态，避免某个 Worker 从未启动时 readiness 仍返回 true。
    /// </summary>
    public void RegisterRequiredWorker(string workerName)
    {
        _requiredWorkers[workerName] = true;
    }

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
            static (_, current, timestamp) => current with
            {
                Status = RealtimeWorkerStatus.Running,
                LastHeartbeatAtMs = timestamp,
                LastError = null
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
                LastHeartbeatAtMs = timestamp,
                LastMessageConsumedAt = timestamp,
                LastOperationSucceededAt = timestamp
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
                SubscriptionConnected = arg.Connected
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

        // Reliability-2：验证所有必需 Worker 都已启动且 Running。
        // 未注册任何必需 Worker 时（如单元测试）不阻断，回退到仅检查已有 Worker。
        var requiredNames = _requiredWorkers.Keys.OrderBy(static n => n, StringComparer.Ordinal).ToArray();
        var allRequiredRunning = requiredNames.Length == 0
            || requiredNames.All(name =>
                _workers.TryGetValue(name, out var w)
                && w.Status == RealtimeWorkerStatus.Running
                && w.LastHeartbeatAtMs is not null
                && now - w.LastHeartbeatAtMs <= timeoutMs);

        // Reliability-2：已有 Worker 全部 Running、心跳未超时、且无队列长时间满。
        var allWorkersHealthy = workers.Length > 0 && workers.All(worker =>
        {
            if (worker.Status != RealtimeWorkerStatus.Running)
                return false;
            if (worker.LastHeartbeatAtMs is not null && now - worker.LastHeartbeatAtMs > timeoutMs)
                return false;
            // 队列持续满超过超时阈值 → 处理卡死，不就绪。
            if (worker.QueueFullSince is long fullSince && now - fullSince > timeoutMs)
                return false;
            return true;
        });

        return new RealtimeReadinessSnapshot(
            allRequiredRunning && allWorkersHealthy,
            workers,
            requiredNames,
            now);
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
