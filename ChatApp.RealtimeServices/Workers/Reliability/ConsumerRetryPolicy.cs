namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// 提取自 IncomingMessageWorker / MessageReceiptWorker 的指数退避公式，
/// 统一订阅中断与 Worker 循环异常的重试延迟语义。
/// </summary>
internal static class ConsumerRetryPolicy
{
    /// <summary>
    /// 订阅中断后的指数退避：500 * 2^min(attempt,6) + jitter，上限 30s。
    /// </summary>
    public static TimeSpan CalculateSubscriptionRetryDelay(int attempt) =>
        TimeSpan.FromMilliseconds(
            Math.Min(30_000, 500 * Math.Pow(2, Math.Min(attempt, 6)))
            + Random.Shared.Next(0, 500));

    /// <summary>
    /// Worker 循环异常后的退避：500 * 2^min(attempt-1,6) + jitter，上限 30s。
    /// </summary>
    public static TimeSpan CalculateWorkerRetryDelay(int attempt) =>
        TimeSpan.FromMilliseconds(
            Math.Min(30_000, 500 * Math.Pow(2, Math.Min(attempt - 1, 6)))
            + Random.Shared.Next(0, 500));
}
