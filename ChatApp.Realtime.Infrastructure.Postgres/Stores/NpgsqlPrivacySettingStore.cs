using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// 二-4：基于 PostgreSQL 的隐私设置查询实现。
/// <para>
/// 查询 <c>public."AspNetUsers"</c> 表的 <c>FriendRequestPolicy</c> 字段：
/// <list type="bullet">
/// <item><c>DisallowAll (2)</c> → 拒绝所有非好友的 DM。</item>
/// <item><c>RequireVerification (0)</c> / <c>AllowAll (1)</c> → 允许 DM（由 <see cref="IDirectMessagePolicy"/> 做进一步校验）。</item>
/// </list>
/// </para>
/// <para>
/// 查询故障时抛出异常，由调用方按 fail-closed 策略处理（拒绝写入）。
/// </para>
/// </summary>
public sealed class NpgsqlPrivacySettingStore : IPrivacySettingStore
{
    private readonly RealtimeDatabaseClient _databaseClient;

    public NpgsqlPrivacySettingStore(RealtimeDatabaseClient databaseClient)
    {
        _databaseClient = databaseClient;
    }

    public async Task<bool> AllowsDirectMessageAsync(
        long userId,
        long targetUserId,
        CancellationToken cancellationToken = default)
    {
        if (userId <= 0 || targetUserId <= 0)
            return false;

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        // 查询接收方（userId）的隐私设置
        await using var command = new NpgsqlCommand(
            """SELECT "FriendRequestPolicy" FROM public."AspNetUsers" WHERE "Id" = @user_id LIMIT 1;""",
            connection);
        command.Parameters.AddWithValue("user_id", userId);

        var scalar = await command.ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false);

        // 用户不存在 → fail-closed 拒绝
        if (scalar is null)
            return false;

        var policy = Convert.ToByte(scalar, System.Globalization.CultureInfo.InvariantCulture);

        // DisallowAll (2) 拒绝所有非好友 DM
        if (policy == 2)
            return false;

        // RequireVerification (0) / AllowAll (1) 允许 DM
        return true;
    }
}
