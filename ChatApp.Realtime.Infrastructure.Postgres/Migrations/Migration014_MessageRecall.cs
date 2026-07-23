using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>消息撤回：recalled_at_ms。</summary>
public sealed class Migration014_MessageRecall : IRealtimeSchemaMigration
{
    public int Version => 14;
    public string Name => "message_recall";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var messages = schema.MessagesTableSql;
        var sql = $"""
                   ALTER TABLE {messages}
                   ADD COLUMN IF NOT EXISTS "recalled_at_ms" bigint NULL;
                   """;
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
