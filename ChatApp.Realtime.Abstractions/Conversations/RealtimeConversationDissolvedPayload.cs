using System.Text.Json.Serialization;

namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 群解散事件 payload。客户端收到后应将会话标记为已解散，
/// 禁止发送新消息，但保留历史读取权限。
/// </summary>
public sealed class RealtimeConversationDissolvedPayload
{
    public const int CurrentPayloadVersion = 1;

    [JsonPropertyName("v")]
    public int PayloadVersion { get; init; } = CurrentPayloadVersion;

    [JsonPropertyName("conversation_id")]
    public required string ConversationId { get; init; }

    [JsonPropertyName("title")]
    public string? Title { get; init; }

    [JsonPropertyName("dissolved_at_ms")]
    public required long DissolvedAtMs { get; init; }

    [JsonPropertyName("actor_user_id")]
    public long? ActorUserId { get; init; }
}
