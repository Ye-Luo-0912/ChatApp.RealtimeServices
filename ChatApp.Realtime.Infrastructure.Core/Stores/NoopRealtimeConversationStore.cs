using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

public sealed class NoopRealtimeConversationStore : IRealtimeConversationStore
{
    public Task<IReadOnlyList<ConversationListItem>> QueryListAsync(
        long userId,
        bool? beforeIsPinned,
        long? beforePinnedAtMs,
        long? beforeLastMessageAtMs,
        string? beforeConversationId,
        int take,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实会话存储，无法查询会话列表。");
    }

    public Task<IReadOnlyList<ConversationListItem>> QueryArchivedListAsync(
        long userId,
        bool? beforeIsPinned,
        long? beforePinnedAtMs,
        long? beforeLastMessageAtMs,
        string? beforeConversationId,
        int take,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实会话存储，无法查询已离群会话列表。");
    }

    public Task<ConversationReadAdvanceResult> AdvanceReadCursorAsync(
        long userId,
        string conversationId,
        long? readAtMs,
        string? readMessageId,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实会话存储，无法更新已读游标。");
    }

    public Task<ConversationMemberPrefsResult> SetMemberPrefsAsync(
        long userId,
        string conversationId,
        bool? pinned,
        bool? muted,
        long? mutedUntilMs,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        throw new InvalidOperationException("未配置真实会话存储，无法更新会话偏好。");
    }
}
