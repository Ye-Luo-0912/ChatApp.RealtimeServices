namespace ChatApp.Realtime.Abstractions.Messaging;

/// <summary>
/// 表示消息处理的结果。该类用于封装消息处理是否成功，以及相关的错误信息或消息ID。
/// 通过此对象，可以了解一个消息在处理过程中的状态及其结果详情。
/// </summary>
/// <remarks>
/// 该类主要用于与消息处理逻辑相关的反馈，特别是在实现IIncomingMessageProcessor接口时，
/// ProcessAsync方法的返回值类型即为此类。它提供了静态方法来快速创建成功或失败的消息处理结果实例。
/// </remarks>
public sealed class MessageProcessResult
{
    public required bool Succeeded { get; init; }
    public string? MessageId { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }

    public static MessageProcessResult Success(string? messageId = null)
    {
        return new MessageProcessResult
        {
            Succeeded = true,
            MessageId = messageId
        };
    }

    public static MessageProcessResult Failed(string errorCode, string errorMessage)
    {
        return new MessageProcessResult
        {
            Succeeded = false,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };
    }
}
