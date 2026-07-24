namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageRecallOptions
{
    public const string SectionName = "MessageRecall";

    /// <summary>发送方可撤回时间窗（分钟），相对 messages.received_at_ms。</summary>
    public int MaxAgeMinutes { get; init; } = 2;

    public long MaxAgeMs =>
        MaxAgeMinutes <= 0
            ? 0
            : (long)TimeSpan.FromMinutes(MaxAgeMinutes).TotalMilliseconds;
}
