using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Conversations;

/// <summary>
/// 会话成员免打扰批量查询处理器（ACCOUNT-OPS-1 离线推送过滤）：
/// 校验查询后透传 store，返回"当前生效免打扰"的成员 Id（升序、去重）。
/// </summary>
public sealed class DefaultConversationMutesQueryProcessor : IConversationMutesQueryProcessor
{
    /// <summary>
    /// 单次查询成员数上限。群成员上限为 200（NpgsqlRealtimeGroupStore.MaxMembersPerGroup），
    /// 离线候选集不会超过该值；留出余量防异常调用方撑爆 SQL ANY 数组。
    /// 超限按查询失败返回，由调用方（Gateway）fail-open 处理。
    /// </summary>
    public const int MaxMemberUserIds = 512;

    private readonly IRealtimeConversationStore _store;

    public DefaultConversationMutesQueryProcessor(IRealtimeConversationStore store)
    {
        _store = store;
    }

    public async Task<ConversationMutesQueryResult> ProcessAsync(
        ConversationMutesQuery query,
        CancellationToken ct = default)
    {
        var validationError = Validate(query);
        if (validationError is not null)
            return validationError;

        var mutedUserIds = await _store
            .QueryMutedMemberIdsAsync(query.ConversationId.Trim(), query.MemberUserIds, ct)
            .ConfigureAwait(false);

        return ConversationMutesQueryResult.Success(query.RequestId, mutedUserIds);
    }

    private static ConversationMutesQueryResult? Validate(ConversationMutesQuery query)
    {
        if (query.RequestId is { Length: > 64 })
            return ConversationMutesQueryResult.Failed(
                query.RequestId,
                "invalid_request_id",
                "请求编号长度不能超过 64。");
        if (string.IsNullOrWhiteSpace(query.ConversationId)
            || query.ConversationId.Trim().Length > ConversationId.MaxLength)
        {
            return ConversationMutesQueryResult.Failed(
                query.RequestId,
                "invalid_conversation_id",
                "会话编号无效。");
        }
        if (query.MemberUserIds is not { Count: > 0 })
        {
            return ConversationMutesQueryResult.Failed(
                query.RequestId,
                "invalid_member_user_ids",
                "成员用户 Id 集合不能为空。");
        }
        if (query.MemberUserIds.Distinct().Any(id => id <= 0))
        {
            return ConversationMutesQueryResult.Failed(
                query.RequestId,
                "invalid_member_user_ids",
                "成员用户编号必须大于 0。");
        }
        if (query.MemberUserIds.Count > MaxMemberUserIds)
        {
            return ConversationMutesQueryResult.Failed(
                query.RequestId,
                "too_many_member_user_ids",
                $"单次查询成员数不能超过 {MaxMemberUserIds}。");
        }

        return null;
    }
}
