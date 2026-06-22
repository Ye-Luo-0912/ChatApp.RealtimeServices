using System.Collections.Concurrent;

namespace ChatApp.Realtime.Infrastructure.Core.Health;

public sealed class RealtimeReadinessState
{
    private readonly ConcurrentDictionary<string, WorkerReadinessSnapshot> _workers = new(StringComparer.Ordinal);

    public void MarkStarted(string workerName)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _workers[workerName] = new WorkerReadinessSnapshot(
            workerName,
            RealtimeWorkerStatus.Running,
            now,
            now,
            null);
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
                null),
            static (_, current, timestamp) => current with
            {
                Status = RealtimeWorkerStatus.Running,
                LastHeartbeatAtMs = timestamp,
                LastError = null
            },
            now);
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
                null),
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
                state.Error),
            static (_, current, state) => current with
            {
                Status = RealtimeWorkerStatus.Faulted,
                LastHeartbeatAtMs = state.Timestamp,
                LastError = state.Error
            },
            (Timestamp: now, Error: ex.Message));
    }

    public RealtimeReadinessSnapshot GetSnapshot()
    {
        var workers = _workers.Values
            .OrderBy(static worker => worker.Name, StringComparer.Ordinal)
            .ToArray();

        return new RealtimeReadinessSnapshot(
            workers.Length > 0 && workers.All(static worker => worker.Status == RealtimeWorkerStatus.Running),
            workers,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }
}

public sealed record RealtimeReadinessSnapshot(
    bool IsReady,
    IReadOnlyCollection<WorkerReadinessSnapshot> Workers,
    long GeneratedAtMs);

public sealed record WorkerReadinessSnapshot(
    string Name,
    RealtimeWorkerStatus Status,
    long? StartedAtMs,
    long? LastHeartbeatAtMs,
    string? LastError);

public enum RealtimeWorkerStatus
{
    Unknown = 0,
    Running = 1,
    Stopped = 2,
    Faulted = 3
}
