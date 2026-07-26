using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Integration.Ephemeral;
using ChatApp.Realtime.Integration.Serialization;
using NATS.Client.Core;

namespace ChatApp.Realtime.Integration;

/// <summary>
/// Realtime 请求/响应客户端：统一封装 NATS request/reply 模板，
/// 提供 9 个查询/变更方法、消息按 Id 查询、Presence 鉴权请求。
/// </summary>
internal sealed class RealtimeRequestClient
{
    private readonly NatsConnectionProvider _connectionProvider;
    private readonly RealtimeIntegrationOptions _options;

    public RealtimeRequestClient(
        NatsConnectionProvider connectionProvider,
        RealtimeIntegrationOptions options)
    {
        _connectionProvider = connectionProvider;
        _options = options;
    }

    public async Task<MessageHistoryPage> QueryMessageHistoryAsync(
        MessageHistoryQuery query,
        CancellationToken ct = default)
    {
        var data = await RequestRawAsync(
            "message_history.request",
            _options.MessageHistoryQueriesSubject,
            RealtimeWireSerializer.Serialize(query),
            query.UserId,
            sessionId: null,
            timeoutMs: _options.HistoryRequestTimeoutMs,
            ct).ConfigureAwait(false);

        if (data is null)
            throw new JsonException("历史消息查询返回了空响应。");

        return RealtimeWireSerializer.DeserializeMessageHistoryPage(data)
               ?? throw new JsonException("历史消息查询响应无法反序列化。");
    }

    public async Task<ConversationListPage> QueryConversationListAsync(
        ConversationListQuery query,
        CancellationToken ct = default)
    {
        var data = await RequestRawAsync(
            "conversation_list.request",
            _options.ConversationListQueriesSubject,
            RealtimeWireSerializer.Serialize(query),
            query.UserId,
            sessionId: null,
            timeoutMs: _options.HistoryRequestTimeoutMs,
            ct).ConfigureAwait(false);

        if (data is null)
            throw new JsonException("会话列表查询返回了空响应。");

        return RealtimeWireSerializer.DeserializeConversationListPage(data)
               ?? throw new JsonException("会话列表查询响应无法反序列化。");
    }

    public async Task<ConversationMarkReadResult> MarkConversationReadAsync(
        ConversationMarkReadCommand command,
        CancellationToken ct = default)
    {
        var data = await RequestRawAsync(
            "conversation_mark_read.request",
            _options.ConversationMarkReadsSubject,
            RealtimeWireSerializer.Serialize(command),
            command.UserId,
            sessionId: null,
            timeoutMs: _options.HistoryRequestTimeoutMs,
            ct).ConfigureAwait(false);

        if (data is null)
            throw new JsonException("会话已读标记返回了空响应。");

        return RealtimeWireSerializer.DeserializeConversationMarkReadResult(data)
               ?? throw new JsonException("会话已读标记响应无法反序列化。");
    }

    public async Task<ConversationSetPrefsResult> SetConversationPrefsAsync(
        ConversationSetPrefsCommand command,
        CancellationToken ct = default)
    {
        var data = await RequestRawAsync(
            "conversation_set_prefs.request",
            _options.ConversationSetPrefsSubject,
            RealtimeWireSerializer.Serialize(command),
            command.UserId,
            sessionId: null,
            timeoutMs: _options.HistoryRequestTimeoutMs,
            ct).ConfigureAwait(false);

        if (data is null)
            throw new JsonException("会话偏好设置返回了空响应。");

        return RealtimeWireSerializer.DeserializeConversationSetPrefsResult(data)
               ?? throw new JsonException("会话偏好设置响应无法反序列化。");
    }

    public async Task<GroupConversationResult> MutateGroupConversationAsync(
        GroupConversationCommand command,
        CancellationToken ct = default)
    {
        var data = await RequestRawAsync(
            "group_conversation.request",
            _options.GroupConversationsSubject,
            RealtimeWireSerializer.Serialize(command),
            command.ActorUserId,
            sessionId: null,
            timeoutMs: _options.HistoryRequestTimeoutMs,
            ct).ConfigureAwait(false);

        if (data is null)
            throw new JsonException("群会话操作返回了空响应。");

        return RealtimeWireSerializer.DeserializeGroupConversationResult(data)
               ?? throw new JsonException("群会话操作响应无法反序列化。");
    }

    public async Task<MessageRecallResult> RecallMessageAsync(
        MessageRecallCommand command,
        CancellationToken ct = default)
    {
        var data = await RequestRawAsync(
            "message_recall.request",
            _options.MessageRecallsSubject,
            RealtimeWireSerializer.Serialize(command),
            command.SenderUserId,
            command.SenderSessionId,
            timeoutMs: _options.HistoryRequestTimeoutMs,
            ct).ConfigureAwait(false);

        if (data is null)
            throw new JsonException("消息撤回返回了空响应。");

        return RealtimeWireSerializer.DeserializeMessageRecallResult(data)
               ?? throw new JsonException("消息撤回响应无法反序列化。");
    }

    public async Task<MessageEditResult> EditMessageAsync(
        MessageEditCommand command,
        CancellationToken ct = default)
    {
        var data = await RequestRawAsync(
            "message_edit.request",
            _options.MessageEditsSubject,
            RealtimeWireSerializer.Serialize(command),
            command.SenderUserId,
            command.SenderSessionId,
            timeoutMs: _options.HistoryRequestTimeoutMs,
            ct).ConfigureAwait(false);

        if (data is null)
            throw new JsonException("消息编辑返回了空响应。");

        return RealtimeWireSerializer.DeserializeMessageEditResult(data)
               ?? throw new JsonException("消息编辑响应无法反序列化。");
    }

    public async Task<MessageReactionResult> ReactToMessageAsync(
        MessageReactionCommand command,
        CancellationToken ct = default)
    {
        var data = await RequestRawAsync(
            "message_reaction.request",
            _options.MessageReactionsSubject,
            RealtimeWireSerializer.Serialize(command),
            command.ActorUserId,
            command.ActorSessionId,
            timeoutMs: _options.HistoryRequestTimeoutMs,
            ct).ConfigureAwait(false);

        if (data is null)
            throw new JsonException("消息反应返回了空响应。");

        return RealtimeWireSerializer.DeserializeMessageReactionResult(data)
               ?? throw new JsonException("消息反应响应无法反序列化。");
    }

    public async Task<SyncBootstrapPage> QuerySyncBootstrapAsync(
        SyncBootstrapQuery query,
        CancellationToken ct = default)
    {
        var data = await RequestRawAsync(
            "sync_bootstrap.request",
            _options.SyncBootstrapQueriesSubject,
            RealtimeWireSerializer.Serialize(query),
            query.UserId,
            sessionId: null,
            timeoutMs: _options.HistoryRequestTimeoutMs,
            ct).ConfigureAwait(false);

        if (data is null)
            throw new JsonException("同步引导查询返回了空响应。");

        return RealtimeWireSerializer.DeserializeSyncBootstrapPage(data)
               ?? throw new JsonException("同步引导查询响应无法反序列化。");
    }

    public async Task<RealtimeHistoryMessage?> TryGetMessageByIdAsync(
        long userId,
        string messageId,
        CancellationToken ct = default)
    {
        var page = await QueryMessageHistoryAsync(
                new MessageHistoryQuery
                {
                    RequestId = Guid.NewGuid().ToString("N")[..16],
                    UserId = userId,
                    MessageId = messageId,
                    Limit = 1,
                },
                ct)
            .ConfigureAwait(false);

        if (!page.Succeeded || page.Items.Count == 0)
            return null;
        return page.Items[0];
    }

    public async Task<PresenceAuthorizeResponse> AuthorizePresenceAsync(
        PresenceAuthorizeQuery query,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        // 超时下限 500ms（避免极端短超时导致 Presence 鉴权频繁失败）；
        // 空响应或反序列化为 null 时返回空 AllowedUserIds（不抛异常）。
        var data = await RequestRawAsync(
            "presence_authorize.request",
            _options.PresenceAuthorizeSubject,
            RealtimeWireSerializer.Serialize(query),
            query.WatcherUserId,
            sessionId: null,
            timeoutMs: Math.Max(500, _options.HistoryRequestTimeoutMs),
            ct).ConfigureAwait(false);

        if (string.IsNullOrWhiteSpace(data))
            return new PresenceAuthorizeResponse { AllowedUserIds = [] };

        return RealtimeWireSerializer.DeserializePresenceAuthorizeResponse(data)
               ?? new PresenceAuthorizeResponse { AllowedUserIds = [] };
    }

    /// <summary>
    /// 统一的 NATS request/reply 模板：创建 Activity、超时控制、身份头注入、异常埋点。
    /// 返回响应 payload；响应为空时返回 <c>null</c>，由调用方决定抛异常还是回退默认值。
    /// </summary>
    private async Task<string?> RequestRawAsync(
        string operation,
        string subject,
        string requestPayload,
        long userId,
        string? sessionId,
        int timeoutMs,
        CancellationToken ct)
    {
        using var activity = RealtimeIntegrationTelemetry.StartClient(operation, subject);
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromMilliseconds(timeoutMs));

            var response = await _connectionProvider.Client.RequestAsync<string, string>(
                    subject,
                    requestPayload,
                    headers: RealtimeIntegrationTelemetry.CreateIdentityHeaders(userId, sessionId),
                    cancellationToken: timeout.Token)
                .ConfigureAwait(false);
            response.EnsureSuccess();

            return string.IsNullOrWhiteSpace(response.Data) ? null : response.Data;
        }
        catch (Exception ex)
        {
            RealtimeIntegrationTelemetry.RecordException(activity, ex);
            throw;
        }
    }
}
