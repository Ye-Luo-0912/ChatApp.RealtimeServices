using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Age-based message retention GC index on <c>received_at_ms</c> for keyset deletes.
/// </summary>
public sealed class Migration020_MessageRetentionAgeIndex : IRealtimeSchemaMigration
{
    public int Version => 20;
    public string Name => "message_retention_age_index";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var messages = schema.MessagesTableSql;
        await using var command = new NpgsqlCommand(
            $"""
             CREATE INDEX IF NOT EXISTS "ix_messages_received_at"
             ON {messages} (received_at_ms, message_id);
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
