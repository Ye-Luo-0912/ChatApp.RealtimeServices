using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeMessageStore : IRealtimeMessageStore
{
    private readonly ILogger<NoopRealtimeMessageStore> _logger;

    public NoopRealtimeMessageStore(ILogger<NoopRealtimeMessageStore> logger)
    {
        _logger = logger;
    }

    public Task<RealtimeMessagePersistResult> SaveAsync(
        RealtimeMessageRecord message,
        Abstractions.Events.RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogCritical(
            "未配置真实消息存储，拒绝确认消息。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            message.MessageId,
            message.SenderUserId,
            message.ReceiverUserId);

        throw new InvalidOperationException("未配置真实消息存储，消息不能被持久化。");
    }

    public Task<MessageReceiptPersistResult> ApplyReceiptAsync(
        MessageReceiptRecord receipt,
        Abstractions.Events.RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogCritical(
            "未配置真实消息存储，拒绝确认消息回执。消息编号={MessageId}；接收用户={ReceiverUserId}",
            receipt.MessageId,
            receipt.ReceiverUserId);

        throw new InvalidOperationException("未配置真实消息存储，消息回执不能被持久化。");
    }

    public Task<MessageRecallPersistResult> ApplyRecallAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        long recalledAtMs,
        long maxAgeMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogCritical(
            "未配置真实消息存储，拒绝撤回。消息编号={MessageId}；发送用户={SenderUserId}",
            messageId,
            senderUserId);

        throw new InvalidOperationException("未配置真实消息存储，消息不能被撤回。");
    }

    public Task<MessageEditPersistResult> ApplyEditAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        string content,
        long editedAtMs,
        long maxAgeMs,
        IReadOnlyList<long>? mentionedUserIds = null,
        IReadOnlyList<string>? mentionedRoles = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogCritical(
            "未配置真实消息存储，拒绝编辑。消息编号={MessageId}；发送用户={SenderUserId}",
            messageId,
            senderUserId);

        throw new InvalidOperationException("未配置真实消息存储，消息不能被编辑。");
    }

    public Task<long> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        ct.ThrowIfCancellationRequested();

        _logger.LogInformation(
            "P0 默认实现跳过用户消息清理。用户={UserId}",
            userId);
        return Task.FromResult(0L);
    }

    public Task EnqueueEventAsync(
        Abstractions.Events.RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(eventToPublish);
        ArgumentException.ThrowIfNullOrWhiteSpace(eventToPublish.EventId);
        ct.ThrowIfCancellationRequested();

        _logger.LogInformation(
            "P0 默认实现跳过 Outbox 入队。事件={EventId}；类型={Type}",
            eventToPublish.EventId,
            eventToPublish.Type);
        return Task.CompletedTask;
    }
}
