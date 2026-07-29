using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class DefaultIncomingMessageProcessor : IIncomingMessageProcessor
{
    private readonly IRealtimeMessageStore _messageStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly RealtimeMetrics _metrics;
    // Perf-3：tombstone 预检查已移除——SaveAsync 事务内的 advisory lock + tombstone 检查是权威的。
    // 字段保留：构造签名兼容，且未来其他 Processor 复用同一依赖图。
    private readonly IUserDeletionTombstoneStore _tombstoneStore;
    private readonly IRealtimeGroupStore _groupStore;
    private readonly ILogger<DefaultIncomingMessageProcessor> _logger;

    public DefaultIncomingMessageProcessor(
        IRealtimeMessageStore messageStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        IUserDeletionTombstoneStore tombstoneStore,
        IRealtimeGroupStore groupStore,
        ILogger<DefaultIncomingMessageProcessor> logger)
    {
        _messageStore = messageStore;
        _outboxSignal = outboxSignal;
        _metrics = metrics;
        _tombstoneStore = tombstoneStore;
        _groupStore = groupStore;
        _logger = logger;
    }

    public async Task<MessageProcessResult> ProcessAsync(
        IncomingMessageCommand command,
        CancellationToken ct = default)
    {
        var validationError = Validate(command);
        if (validationError is not null)
        {
            _metrics.RecordProcessingFailure("validation");
            return validationError;
        }

        // P0-5：在入口生成服务端权威时间，覆盖任何客户端提供的值。
        // 下游所有排序、序列、tip、retention 决策均使用 ServerReceivedAtMs，
        // 客户端上报的 ClientOccurredAtMs 仅用于诊断/展示。
        command = command with { ServerReceivedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() };

        // Perf-3：移除事务外的 tombstone 预检查（连接 #1）。
        // SaveAsync 事务内的 UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveAsync
        // 已做权威检查，预检查只会引入 TOCTOU 竞态与额外数据库往返。
        // 用户已注销时 SaveAsync 返回 UserDeleted，由下方分支统一处理。

        string conversationId;
        long receiverUserId;
        var explicitConversationId = string.IsNullOrWhiteSpace(command.ConversationId)
            ? null
            : command.ConversationId.Trim();

        var isGroupMessage = explicitConversationId is not null
                             && ConversationId.IsGroup(explicitConversationId);

        if (isGroupMessage)
        {
            conversationId = explicitConversationId!;
            receiverUserId = 0;
            // Perf-2：删除 Processor 的群成员预检查。该查询不具备事务权威性
            // （查询成功后用户仍可能立即被移除），且每条群消息多一次数据库往返。
            // 由 NpgsqlRealtimeMessageStore.SaveAsync 在写事务内加载成员并验证，
            // 失败时返回 IsNotAllowed，由下方分支统一处理。
        }
        else
        {
            if (command.ReceiverUserId <= 0)
            {
                _metrics.RecordProcessingFailure("validation");
                return MessageProcessResult.Failed(
                    "invalid_user_id",
                    "发送方和接收方用户编号必须大于 0。");
            }

            if (command.SenderUserId == command.ReceiverUserId)
            {
                _metrics.RecordProcessingFailure("validation");
                return MessageProcessResult.Failed(
                    "invalid_self_chat",
                    "单聊发送方与接收方不能为同一用户。");
            }

            conversationId = ConversationId.CreateDirect(
                command.SenderUserId,
                command.ReceiverUserId);
            receiverUserId = command.ReceiverUserId;
        }

        // Mentions 业务闭环：去重 / 排除自身 / 截断 / 群成员校验 / @all|@admin 权限校验。
        // 所有违规一律静默过滤——不拒绝整条消息，符合 Realtime"消息必达"语义。
        // 单聊场景下：仅做基本去重与排除自身（mention 通常无意义但保留透传）。
        // 群聊场景下：进一步用群活跃成员集合过滤 MentionedUserIds，并按发送者角色过滤 @all|@admin。
        var sanitizedMentions = await SanitizeMentionsAsync(
            command,
            conversationId,
            isGroupMessage,
            ct).ConfigureAwait(false);

        // P0-10：在 sanitization 之前基于原始请求计算 fingerprint。
        // 使用 command 的原始 MentionedUserIds/MentionedRoles（未 sanitized），
        // 避免相同请求因当前成员/角色变化导致 fingerprint 不同（误判冲突）。
        // Reply/Forward 的 sender 与 preview 也纳入指纹，防止仅引用元数据变化不触发冲突。
        var requestFingerprint = RealtimeMessageFingerprint.Compute(
            receiverUserId,
            command.Content,
            command.AttachmentIds,
            conversationId,
            command.ReplyToMessageId,
            command.ForwardedFromMessageId,
            command.MentionedUserIds,
            command.MentionedRoles,
            command.ReplyToSenderUserId,
            command.ReplyToPreview,
            command.ForwardedFromSenderUserId,
            command.ForwardedFromPreview);

        var record = new RealtimeMessageRecord
        {
            MessageId = command.CommandId,
            ClientMessageId = command.ClientMessageId,
            SenderUserId = command.SenderUserId,
            SenderSessionId = command.SenderSessionId,
            ReceiverUserId = receiverUserId,
            ConversationId = conversationId,
            Content = command.Content,
            AttachmentIds = command.AttachmentIds,
            ReplyToMessageId = command.ReplyToMessageId,
            ReplyToSenderUserId = command.ReplyToSenderUserId,
            ReplyToPreview = command.ReplyToPreview,
            ForwardedFromMessageId = command.ForwardedFromMessageId,
            ForwardedFromSenderUserId = command.ForwardedFromSenderUserId,
            ForwardedFromPreview = command.ForwardedFromPreview,
            MentionedUserIds = sanitizedMentions.UserIds,
            MentionedRoles = sanitizedMentions.Roles,
            RequestFingerprint = requestFingerprint,
            ReceivedAtMs = command.ServerReceivedAtMs
        };

        var evt = new RealtimeEvent
        {
            EventId = ConversationId.IsGroup(conversationId)
                ? MessageEventIdFactory.CreateMessageReceivedEventId(
                    command.SenderUserId,
                    command.ClientMessageId,
                    command.SenderUserId)
                : MessageEventIdFactory.CreateMessageReceivedEventId(
                    command.SenderUserId,
                    command.ClientMessageId),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = ConversationId.IsGroup(conversationId)
                ? command.SenderUserId
                : command.ReceiverUserId,
            ActorUserId = command.SenderUserId,
            MessageId = command.CommandId,
            SessionId = command.SenderSessionId,
            // P1-4：直接传 payload 对象给 Store，由 Store 在附件绑定后调用
            // EnrichChatMessagePayload 一次性物化为 PayloadJson，省去 Processor
            // 序列化 + Store 反序列化的重复工作。Outbox 仅看到物化后的 PayloadJson。
            Payload = new RealtimeChatMessagePayload
            {
                MessageId = command.CommandId,
                ClientMessageId = command.ClientMessageId,
                SenderUserId = command.SenderUserId,
                SenderSessionId = command.SenderSessionId,
                ReceiverUserId = receiverUserId,
                ConversationId = conversationId,
                Content = command.Content,
                ReceivedAtMs = command.ServerReceivedAtMs,
                ReplyToMessageId = command.ReplyToMessageId,
                ReplyToSenderUserId = command.ReplyToSenderUserId,
                ReplyToPreview = command.ReplyToPreview,
                ForwardedFromMessageId = command.ForwardedFromMessageId,
                ForwardedFromSenderUserId = command.ForwardedFromSenderUserId,
                ForwardedFromPreview = command.ForwardedFromPreview,
                MentionedUserIds = sanitizedMentions.UserIds,
                MentionedRoles = sanitizedMentions.Roles
            },
            OccurredAtMs = command.ServerReceivedAtMs,
            TraceParent = RealtimeTraceContext.CaptureTraceParent(),
            TraceState = RealtimeTraceContext.CaptureTraceState()
        };

        var persisted = await _messageStore.SaveAsync(record, evt, ct).ConfigureAwait(false);
        if (persisted.IsConflict)
        {
            _metrics.RecordIdempotencyConflict();
            _metrics.RecordProcessingFailure("idempotency_conflict");
            _logger.LogWarning(
                "入站消息幂等键内容冲突。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；已有消息={MessageId}",
                record.ClientMessageId,
                record.SenderUserId,
                persisted.MessageId);
            // Perf-3：账本回填已由 SaveAsync 事务内完成（Conflict 分支）。
            return MessageProcessResult.Failed(
                "idempotency_conflict",
                "相同客户端消息编号已存在但内容不一致。",
                MessageFailureKind.Permanent);
        }

        if (persisted.IsAttachmentBindFailed)
        {
            _metrics.RecordProcessingFailure("attachment_bind_failed");
            _logger.LogWarning(
                "入站消息附件绑定失败。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}；消息={MessageId}",
                record.ClientMessageId,
                record.SenderUserId,
                persisted.MessageId);
            // 附件绑定失败不记录账本：重试可能成功（附件状态可能变化）。
            return MessageProcessResult.Failed(
                "attachment_bind_failed",
                "附件不存在、未确认或不属于发送方，消息未写入。",
                MessageFailureKind.Permanent);
        }

        if (persisted.IsNotAllowed)
        {
            _metrics.RecordProcessingFailure("not_member");
            // 权限失败不记录账本：重试可能成功（成员关系可能变化）。
            return MessageProcessResult.Failed(
                "forbidden",
                "无权在该会话发送消息。",
                MessageFailureKind.Permanent);
        }

        if (persisted.IsUserDeleted)
        {
            _metrics.RecordProcessingFailure("user_deleted");
            // P0-2：事务内 advisory lock 检测到用户正在删除/已删除，不记录账本。
            return MessageProcessResult.Failed(
                "user_deleted",
                "用户已注销，消息未写入。",
                MessageFailureKind.Permanent);
        }

        _outboxSignal.Notify();

        if (!persisted.IsNew)
        {
            _logger.LogDebug(
                "重复入站消息已完成幂等处理。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
                persisted.MessageId,
                record.SenderUserId,
                record.ReceiverUserId);

            _metrics.RecordDuplicate();
            // Perf-3：账本回填已由 SaveAsync 事务内完成（Duplicate 分支）。
            return MessageProcessResult.Success(persisted.MessageId);
        }

        _metrics.RecordPersisted();
        // Perf-3：账本 Created 记录已由 SaveAsync 事务内完成。

        _logger.LogDebug(
            "入站消息已处理。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
            record.MessageId,
            record.SenderUserId,
            record.ReceiverUserId);

        return MessageProcessResult.Success(record.MessageId);
    }

    /// <summary>
    /// 规范化 mention 字段。
    /// <para>
    /// P0-3：群聊场景不再无条件加载全量成员列表。
    /// </para>
    /// <list type="bullet">
    /// <item>无 mention（MentionedUserIds 与 MentionedRoles 均空）：跳过所有群存储查询，直接返回空 + isManager=false。</item>
    /// <item>有 mention：仅查询发送者角色（GetMemberRoleAsync，O(1)）+ 最多 50 个 mentioned users 的成员资格（ValidateMembersAsync，单条 SQL）。</item>
    /// </list>
    /// <para>
    /// 单聊场景下仅做基本去重 + 排除自身；mention 通常无意义但保留透传。
    /// </para>
    /// </summary>
    private async Task<SanitizedMentions> SanitizeMentionsAsync(
        IncomingMessageCommand command,
        string conversationId,
        bool isGroupMessage,
        CancellationToken ct)
    {
        // 单聊或无 mention：默认 sender 视为"管理员"（@all/@admin 在单聊无意义但允许透传），
        // 不做群成员过滤。
        if (!isGroupMessage)
        {
            var directUserIds = MentionValidator.AsReadOnly(
                MentionValidator.NormalizeUserIds(command.MentionedUserIds, command.SenderUserId));
            var directRoles = MentionValidator.AsReadOnly(
                MentionValidator.NormalizeRoles(command.MentionedRoles, isManager: true));
            return new SanitizedMentions(directUserIds, directRoles);
        }

        // P0-3：群聊无 mention 时跳过全部群存储查询，直接返回空 + isManager=false。
        // 下游 SaveAsync 的 TryAdvanceGroupSequenceAsync 内嵌 EXISTS 权威成员校验，
        // 无需在此预检查发送者成员资格。
        var hasUserMentions = command.MentionedUserIds is { Count: > 0 };
        var hasRoleMentions = command.MentionedRoles is { Count: > 0 };
        if (!hasUserMentions && !hasRoleMentions)
        {
            return new SanitizedMentions(
                MentionValidator.AsReadOnly((long[]?)null),
                MentionValidator.AsReadOnly((string[]?)null));
        }

        // 群聊有 mention：查询发送者角色（O(1)）以判定 isManager。
        // 若发送方不是活跃成员（role=null），退化为基本去重 + isManager=false，
        // 由下游 SaveAsync 的权威成员校验拒绝消息。
        ConversationMemberRole? senderRole;
        try
        {
            senderRole = await _groupStore
                .GetMemberRoleAsync(command.SenderUserId, conversationId, ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // 群存储异常不应阻塞消息写入；退化为基本去重，下游 SaveAsync 会做权威校验。
            _logger.LogWarning(
                ex,
                "群成员角色查询失败，退化为基本 mention 规范化。会话={ConversationId}；发送用户={SenderUserId}",
                conversationId,
                command.SenderUserId);
            var fallbackUserIds = MentionValidator.AsReadOnly(
                MentionValidator.NormalizeUserIds(command.MentionedUserIds, command.SenderUserId));
            var fallbackRoles = MentionValidator.AsReadOnly(
                MentionValidator.NormalizeRoles(command.MentionedRoles, isManager: false));
            return new SanitizedMentions(fallbackUserIds, fallbackRoles);
        }

        if (senderRole is null)
        {
            // 发送方不是群成员：基本去重 + 视为非管理员。下游 SaveAsync 会拒绝。
            var nonMemberUserIds = MentionValidator.AsReadOnly(
                MentionValidator.NormalizeUserIds(command.MentionedUserIds, command.SenderUserId));
            var nonMemberRoles = MentionValidator.AsReadOnly(
                MentionValidator.NormalizeRoles(command.MentionedRoles, isManager: false));
            return new SanitizedMentions(nonMemberUserIds, nonMemberRoles);
        }

        var isManager = senderRole.Value == ConversationMemberRole.Owner
                        || senderRole.Value == ConversationMemberRole.Admin;

        // P0-3：仅当有 MentionedUserIds 时才查询 mentioned users 的成员资格（单条 SQL）。
        // 将发送者加入成员集合保证 NormalizeUserIds 的过滤分支生效（集合非空），
        // 发送者自身由 NormalizeUserIds 的"排除自身"规则移除。
        HashSet<long>? memberSet = null;
        if (hasUserMentions)
        {
            try
            {
                var validMembers = await _groupStore
                    .ValidateMembersAsync(conversationId, command.MentionedUserIds!, ct)
                    .ConfigureAwait(false);
                memberSet = new HashSet<long>(validMembers.Count + 1) { command.SenderUserId };
                foreach (var id in validMembers)
                    memberSet.Add(id);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // 校验失败不阻塞：退化为不做成员过滤（仅基本去重 + 排除自身）。
                _logger.LogWarning(
                    ex,
                    "群成员资格批量校验失败，退化为不做成员过滤。会话={ConversationId}；发送用户={SenderUserId}",
                    conversationId,
                    command.SenderUserId);
            }
        }

        var sanitizedUserIds = MentionValidator.AsReadOnly(
            MentionValidator.NormalizeUserIds(
                command.MentionedUserIds,
                command.SenderUserId,
                memberSet));
        var sanitizedRoles = MentionValidator.AsReadOnly(
            MentionValidator.NormalizeRoles(command.MentionedRoles, isManager));
        return new SanitizedMentions(sanitizedUserIds, sanitizedRoles);
    }

    private static MessageProcessResult? Validate(IncomingMessageCommand command)
    {
        if (string.IsNullOrWhiteSpace(command.CommandId) || command.CommandId.Length > 64)
            return MessageProcessResult.Failed("invalid_command_id", "命令编号不能为空且长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(command.ClientMessageId) || command.ClientMessageId.Length > 128)
            return MessageProcessResult.Failed("invalid_client_message_id", "客户端消息编号不能为空且长度不能超过 128。");
        if (command.SenderUserId <= 0)
            return MessageProcessResult.Failed("invalid_user_id", "发送方用户编号必须大于 0。");

        var conversationId = string.IsNullOrWhiteSpace(command.ConversationId)
            ? null
            : command.ConversationId.Trim();
        if (conversationId is not null)
        {
            if (conversationId.Length > ConversationId.MaxLength
                || (!ConversationId.IsGroup(conversationId) && !ConversationId.IsDirect(conversationId)))
            {
                return MessageProcessResult.Failed("invalid_conversation_id", "会话编号无效。");
            }
        }

        if (string.IsNullOrWhiteSpace(command.SenderSessionId) || command.SenderSessionId.Length > 128)
            return MessageProcessResult.Failed("invalid_session_id", "发送会话编号不能为空且长度不能超过 128。");
        if (string.IsNullOrWhiteSpace(command.Content)
            && command.AttachmentIds is not { Count: > 0 })
            return MessageProcessResult.Failed("empty_content", "入站消息内容不能为空。");
        // P0-8：修复 attachment-only 消息 Content=null 时访问 .Length 的 NRE
        if ((command.Content?.Length ?? 0) > 65_536)
            return MessageProcessResult.Failed("content_too_large", "入站消息内容不能超过 65536 个字符。");
        if (command.AttachmentIds is { Count: > 0 })
        {
            if (command.AttachmentIds.Count > 32)
                return MessageProcessResult.Failed(
                    "too_many_attachments",
                    "单条消息附件数不能超过 32。");
            foreach (var id in command.AttachmentIds)
            {
                if (string.IsNullOrWhiteSpace(id) || id.Length > 64)
                    return MessageProcessResult.Failed(
                        "invalid_attachment_id",
                        "附件编号不能为空且长度不能超过 64。");
            }
        }

        if (!string.IsNullOrWhiteSpace(command.ReplyToMessageId))
        {
            if (command.ReplyToMessageId.Length > 64)
                return MessageProcessResult.Failed(
                    "invalid_reply_to_message_id",
                    "回复目标消息编号长度不能超过 64。");
            if (command.ReplyToSenderUserId is null or <= 0)
                return MessageProcessResult.Failed(
                    "invalid_reply_to_sender",
                    "回复目标发送方用户编号必须大于 0。");
            if (command.ReplyToPreview is { Length: > 256 })
                return MessageProcessResult.Failed(
                    "invalid_reply_to_preview",
                    "回复预览长度不能超过 256。");
        }
        else if (command.ReplyToSenderUserId is not null
                 || !string.IsNullOrWhiteSpace(command.ReplyToPreview))
        {
            return MessageProcessResult.Failed(
                "invalid_reply_to",
                "缺少回复目标消息编号时不能携带回复元数据。");
        }

        if (!string.IsNullOrWhiteSpace(command.ForwardedFromMessageId)
            && !string.IsNullOrWhiteSpace(command.ReplyToMessageId))
        {
            return MessageProcessResult.Failed(
                "invalid_reply_and_forward",
                "同一条消息不能同时回复与转发。");
        }

        if (!string.IsNullOrWhiteSpace(command.ForwardedFromMessageId))
        {
            if (command.ForwardedFromMessageId.Length > 64)
                return MessageProcessResult.Failed(
                    "invalid_forwarded_from_message_id",
                    "转发原消息编号长度不能超过 64。");
            if (command.ForwardedFromSenderUserId is null or <= 0)
                return MessageProcessResult.Failed(
                    "invalid_forwarded_from_sender",
                    "转发原发送方用户编号必须大于 0。");
            if (command.ForwardedFromPreview is { Length: > 256 })
                return MessageProcessResult.Failed(
                    "invalid_forwarded_from_preview",
                    "转发预览长度不能超过 256。");
        }
        else if (command.ForwardedFromSenderUserId is not null
                 || !string.IsNullOrWhiteSpace(command.ForwardedFromPreview))
        {
            return MessageProcessResult.Failed(
                "invalid_forwarded_from",
                "缺少转发原消息编号时不能携带转发元数据。");
        }

        // Mentions 业务校验已迁移到 MentionValidator（在 SanitizeMentionsAsync 中调用）：
        // - 数量超限：截断而非拒绝
        // - 重复用户/角色：去重而非拒绝
        // - @自己：静默移除
        // - 非群成员：静默移除（群聊场景）
        // - @all/@admin 非管理员发送：静默移除（群聊场景）

        return null;
    }

    private sealed record SanitizedMentions(
        IReadOnlyList<long>? UserIds,
        IReadOnlyList<string>? Roles);
}
