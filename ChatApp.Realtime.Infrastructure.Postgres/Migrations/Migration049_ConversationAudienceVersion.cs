using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 四-1：会话受众版本号。
/// <para>
/// 在 conversations 表添加 audience_version 列（bigint NOT NULL DEFAULT 1）。
/// 每次群成员变更（加人/踢人/离群/解散）时递增，Gateway 据此判断本地 audience 缓存是否过期。
/// </para>
/// </summary>
public sealed class Migration049_ConversationAudienceVersion : IRealtimeSchemaMigration
{
    public int Version => 49;
    public string Name => "conversation_audience_version";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var conversations = schema.ConversationsTableSql;

        await using var command = new NpgsqlCommand(
            $"""
            ALTER TABLE {conversations}
                ADD COLUMN IF NOT EXISTS "audience_version" bigint NOT NULL DEFAULT 1;
            """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}