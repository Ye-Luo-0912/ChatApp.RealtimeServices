namespace ChatApp.Realtime.Abstractions.Relationships;

public sealed class RelationshipCommandResult
{
    public required string RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }
    public RelationshipOperation? Operation { get; init; }
    public long? TargetUserId { get; init; }
    public string? ResourceId { get; init; }

    public static RelationshipCommandResult Success(
        string requestId,
        RelationshipOperation operation,
        long? targetUserId = null,
        string? resourceId = null) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            Operation = operation,
            TargetUserId = targetUserId,
            ResourceId = resourceId
        };

    public static RelationshipCommandResult Failed(
        string requestId,
        string errorCode,
        string errorMessage) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };

    public static RelationshipCommandResult ServerBusy(
        string requestId,
        int retryAfterMs,
        string queueKind) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = "server_busy",
            ErrorMessage = "服务繁忙，请稍后重试。",
            RetryAfterMs = retryAfterMs,
            QueueKind = queueKind
        };
}
