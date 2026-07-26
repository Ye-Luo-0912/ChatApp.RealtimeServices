using ChatApp.Realtime.Infrastructure.Core.Health;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// 提取自 IncomingMessageWorker / MessageReceiptWorker 的共享心跳与取消抑制逻辑。
/// </summary>
internal static class WorkerHeartbeat
{
    /// <summary>
    /// 以 5 秒为周期持续向 <see cref="RealtimeReadinessState"/> 报告指定 Worker 的心跳，
    /// 直到传入的 <paramref name="ct"/> 取消。
    /// </summary>
    public static async Task RunAsync(
        RealtimeReadinessState readinessState,
        string workerName,
        CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false))
            readinessState.MarkHeartbeat(workerName);
    }

    /// <summary>
    /// 等待指定任务完成，吞掉 <see cref="OperationCanceledException"/>。
    /// 用于 shutdown 阶段安全等待后台任务退出。
    /// </summary>
    public static async Task SuppressCancellationAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }
}
