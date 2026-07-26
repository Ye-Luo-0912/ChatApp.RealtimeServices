using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>消息 @ 提及字段（mentioned_user_ids / mentioned_roles）。</summary>
public sealed class Migration021_MessageMentions : IRealtimeSchemaMigration
{
    public int Version => 21;
    public string Name => "message_mentions";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var messages = schema.MessagesTableSql;
        var commands = new[]
        {
            $"""
             ALTER TABLE {messages}
             ADD COLUMN IF NOT EXISTS "mentioned_user_ids" bigint[] NULL;
             """,
            $"""
             ALTER TABLE {messages}
             ADD COLUMN IF NOT EXISTS "mentioned_roles" text[] NULL;
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}