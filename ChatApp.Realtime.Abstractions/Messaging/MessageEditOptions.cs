namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageEditOptions
{
    public const string SectionName = "MessageEdit";

    /// <summary>发送方可编辑时间窗（分钟），相对 messages.received_at_ms。</summary>
    public int MaxAgeMinutes { get; init; } = 15;

    public long MaxAgeMs =>
        MaxAgeMinutes <= 0
            ? 0
            : (long)TimeSpan.FromMinutes(MaxAgeMinutes).TotalMilliseconds;
}
