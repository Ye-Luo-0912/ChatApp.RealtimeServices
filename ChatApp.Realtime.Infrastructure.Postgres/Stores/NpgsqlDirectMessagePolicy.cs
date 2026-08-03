using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// 二-3：基于 PostgreSQL 的 DM 策略实现。
/// <para>
/// 查询 <c>public."T_UserFriendEntry"</c> 表，确认发送方与接收方是否为好友关系。
/// 策略规则：
/// <list type="bullet">
/// <item>互为好友（双向好友关系）→ 允许 DM。</item>
/// <item>非好友 → 由 <see cref="IPrivacySettingStore"/> 决定。</item>
/// </list>
/// </para>
/// <para>
/// 查询故障时抛出异常，由调用方按 fail-closed 策略处理（拒绝写入）。
/// </para>
/// </summary>
public sealed class NpgsqlDirectMessagePolicy : IDirectMessagePolicy
{
    private readonly RealtimeDatabaseClient _databaseClient;

    public NpgsqlDirectMessagePolicy(RealtimeDatabaseClient databaseClient)
    {
        _databaseClient = databaseClient;
    }

    public async Task<DirectMessagePolicyResult> CheckAsync(
        long senderUserId,
        long receiverUserId,
        CancellationToken cancellationToken = default)
    {
        if (senderUserId <= 0 || receiverUserId <= 0)
            return new DirectMessagePolicyResult { Allowed = false, ErrorCode = "invalid_user" };

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        // 检查双向好友关系：senderUserId 是 receiverUserId 的好友 且 receiverUserId 是 senderUserId 的好友
        await using var command = new NpgsqlCommand(
            """
            SELECT COUNT(*)::int
            FROM public."T_UserFriendEntry"
            WHERE ("UserId" = @sender_id AND "FriendId" = @receiver_id AND NOT "IsDeleted")
               OR ("UserId" = @receiver_id AND "FriendId" = @sender_id AND NOT "IsDeleted");
            """,
            connection);
        command.Parameters.AddWithValue("sender_id", senderUserId);
        command.Parameters.AddWithValue("receiver_id", receiverUserId);

        var count = (int?)await command.ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false);

        // 双向好友关系完整（count == 2）才视为好友
        var areFriends = count >= 2;

        if (!areFriends)
        {
            return new DirectMessagePolicyResult
            {
                Allowed = false,
                ErrorCode = "not_friend",
                ErrorMessage = "仅好友可发送消息。"
            };
        }

        return new DirectMessagePolicyResult { Allowed = true };
    }
}
