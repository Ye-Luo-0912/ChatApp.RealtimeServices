namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>已解析/钳制的同步水位（必为会话内真实消息或 tip）。</summary>
public sealed class ResolvedSyncWatermark
{
    public required string ConversationId { get; init; }
    public required long AfterReceivedAtMs { get; init; }
    public required string AfterMessageId { get; init; }
}
