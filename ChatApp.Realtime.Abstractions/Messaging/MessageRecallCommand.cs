namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageRecallCommand
{
    public required string RequestId { get; init; }
    public required string MessageId { get; init; }
    public long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public long OccurredAtMs { get; init; }

    /// <summary>
    /// 服务端变更时间，由处理器入口生成，用于撤回窗口判断和 changed_at_ms 推进。
    /// 客户端上报的 <see cref="OccurredAtMs"/> 仅用于诊断/展示。
    /// </summary>
    public long ServerMutationAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
