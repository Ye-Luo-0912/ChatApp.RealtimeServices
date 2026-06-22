using System.Collections.Concurrent;

namespace ChatApp.Realtime.Infrastructure.Health;

public sealed class RealtimeReadinessState
{
    private readonly ConcurrentDictionary<string, WorkerReadinessSnapshot> _workers = new(StringComparer.Ordinal);

    /// <summary>
    /// 标记指定工作器为已启动状态，更新其状态为运行中，并记录启动时间。
    /// </summary>
    /// <param name="workerName">工作器的名称。</param>
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

    /// <summary>
    /// 标记指定工作器的心跳，更新其状态为运行中，并记录最后一次心跳的时间。
    /// </summary>
    /// <param name="workerName">工作器的名称。</param>
    public void MarkHeartbeat(string workerName)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        //
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

    /// <summary>
    /// 标记指定工作器为已停止状态，更新其状态为停止，并记录最后一次心跳时间。
    /// </summary>
    /// <param name="workerName">工作器的名称。</param>
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

    /// <summary>
    /// 标记指定工作器为故障状态，更新其状态为故障，并记录错误信息。
    /// </summary>
    /// <param name="workerName">工作器的名称。</param>
    /// <param name="ex">发生的异常。</param>
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

    /// <summary>
    /// 获取当前实时就绪状态的快照，包括所有工作器的状态及生成时间。
    /// </summary>
    /// <returns>包含是否就绪、所有工作器的状态信息以及快照生成时间戳的实时就绪状态快照。</returns>
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

/// <summary>
/// 表示实时就绪状态的快照。该类封装了系统整体是否就绪的信息，所有工作进程的状态信息以及快照生成的时间戳。
/// </summary>
/// <param name="IsReady">指示整个系统是否处于就绪状态，当所有工作进程都在运行时为true。</param>
/// <param name="Workers">包含当前所有工作进程状态信息的集合。</param>
/// <param name="GeneratedAtMs">快照生成的时间戳（以毫秒为单位）。</param>
public sealed record RealtimeReadinessSnapshot(
    bool IsReady,
    IReadOnlyCollection<WorkerReadinessSnapshot> Workers,
    long GeneratedAtMs);

/// <summary>
/// 表示实时工作进程的就绪状态快照。该类封装了工作进程的状态信息，包括名称、当前状态、启动时间、最后心跳时间以及最后一次错误信息。
/// </summary>
/// <param name="Name">工作进程的唯一标识符。</param>
/// <param name="Status">工作进程的当前状态，可以是运行中、已停止或出错等。</param>
/// <param name="StartedAtMs">工作进程启动的时间戳（以毫秒为单位），如果尚未启动则为null。</param>
/// <param name="LastHeartbeatAtMs">工作进程最后一次发送心跳信号的时间戳（以毫秒为单位），用于监测其活跃状态。</param>
/// <param name="LastError">工作进程中发生的最近一次错误的信息，如果没有错误发生则为null。</param>
public sealed record WorkerReadinessSnapshot(
    string Name,
    RealtimeWorkerStatus Status,
    long? StartedAtMs,
    long? LastHeartbeatAtMs,
    string? LastError);

/// <summary>
/// 表示实时工作器的状态。
/// </summary>
public enum RealtimeWorkerStatus
{
    /// <summary>
    /// 表示工作器状态未知。这可能意味着工作器尚未启动，或者没有正确注册到系统中。
    /// 在这种状态下，系统无法确定工作器的具体情况或健康状况。
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// 表示工作器当前处于运行状态。这通常意味着工作器已经成功启动，并且正在执行其预定的任务。
    /// 在这种状态下，工作器被认为是健康和可用的，可以处理请求或任务。
    /// </summary>
    Running = 1,

    /// <summary>
    /// 表示工作器已停止运行。在这种状态下，工作器不再处理任何新的请求或任务。
    /// 通常，当工作器被正常关闭或者在某些条件下自动停止时，会进入此状态。
    /// </summary>
    Stopped = 2,

    /// <summary>
    /// 表示工作器处于故障状态。这通常意味着工作器遇到了无法自动恢复的错误，或者运行时出现了异常。
    /// 在这种状态下，工作器可能需要人工干预或重启才能恢复正常运作。
    /// </summary>
    Faulted = 3
}
