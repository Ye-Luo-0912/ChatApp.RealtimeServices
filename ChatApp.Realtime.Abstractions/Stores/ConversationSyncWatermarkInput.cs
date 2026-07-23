namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>同步水位解析输入。</summary>
public sealed class ConversationSyncWatermarkInput
{
    public required string ConversationId { get; init; }
    public required long AfterReceivedAtMs { get; init; }
    public required string AfterMessageId { get; init; }

    /// <summary>可选：列表页已知 tip，避免二次查询。</summary>
    public long? TipReceivedAtMs { get; init; }

    public string? TipMessageId { get; init; }
}
