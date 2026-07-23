using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Outbox 增加 typed <c>target_user_id</c> / <c>event_type</c>，并�?payload JSON 回填�?
/// 以便用户清理按精确列匹配，而不再依�?JSON LIKE�?
/// </summary>
public sealed class Migration002_OutboxTypedTargetColumns : IRealtimeSchemaMigration
{
    public int Version => 2;
    public string Name => "outbox_typed_target_columns";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var outbox = schema.OutboxTableSql;

        var commands = new[]
        {
            $"ALTER TABLE {outbox} ADD COLUMN IF NOT EXISTS \"target_user_id\" bigint NULL;",
            $"ALTER TABLE {outbox} ADD COLUMN IF NOT EXISTS \"event_type\" smallint NULL;",
            // jsonb 解析对空白与属性顺序不敏感，避免旧 LIKE 前缀误伤（如 user 12 误删 123）�?
            $"""
             UPDATE {outbox}
             SET
                 "target_user_id" = COALESCE(
                     "target_user_id",
                     NULLIF(BTRIM("payload_json"::jsonb ->> 'TargetUserId'), '')::bigint),
                 "event_type" = COALESCE(
                     "event_type",
                     NULLIF(BTRIM("payload_json"::jsonb ->> 'Type'), '')::smallint)
             WHERE "target_user_id" IS NULL OR "event_type" IS NULL;
             """,
            // 无法�?payload 解析的历史脏行：落到 0，保证后�?NOT NULL；userId>0 的清理不会误删�?
            $"""
             UPDATE {outbox}
             SET
                 "target_user_id" = COALESCE("target_user_id", 0),
                 "event_type" = COALESCE("event_type", 0)
             WHERE "target_user_id" IS NULL OR "event_type" IS NULL;
             """,
            $"ALTER TABLE {outbox} ALTER COLUMN \"target_user_id\" SET DEFAULT 0;",
            $"ALTER TABLE {outbox} ALTER COLUMN \"event_type\" SET DEFAULT 0;",
            $"ALTER TABLE {outbox} ALTER COLUMN \"target_user_id\" SET NOT NULL;",
            $"ALTER TABLE {outbox} ALTER COLUMN \"event_type\" SET NOT NULL;",
            $"CREATE INDEX IF NOT EXISTS \"ix_outbox_target_user_id\" ON {outbox} (\"target_user_id\");",
            $"CREATE INDEX IF NOT EXISTS \"ix_outbox_target_user_event_type\" ON {outbox} (\"target_user_id\", \"event_type\");"
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
