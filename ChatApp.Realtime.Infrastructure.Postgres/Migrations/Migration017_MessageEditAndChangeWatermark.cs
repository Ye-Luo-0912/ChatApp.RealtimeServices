using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 消息编辑 + 变更水位 + 编辑/撤回请求幂等账本。
/// </summary>
public sealed class Migration017_MessageEditAndChangeWatermark : IRealtimeSchemaMigration
{
    public int Version => 17;
    public string Name => "message_edit_and_change_watermark";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var messages = schema.MessagesTableSql;
        var mutationRequests = $"{schema.QuotedSchema}.\"message_mutation_requests\"";

        await using (var alter = new NpgsqlCommand(
                         $"""
                          ALTER TABLE {messages}
                          ADD COLUMN IF NOT EXISTS "edit_version" integer NOT NULL DEFAULT 1;
                          ALTER TABLE {messages}
                          ADD COLUMN IF NOT EXISTS "edited_at_ms" bigint NULL;
                          ALTER TABLE {messages}
                          ADD COLUMN IF NOT EXISTS "changed_at_ms" bigint NULL;
                          """,
                         connection,
                         transaction))
        {
            await alter.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var backfill = new NpgsqlCommand(
                         $"""
                          UPDATE {messages}
                          SET changed_at_ms = COALESCE(recalled_at_ms, edited_at_ms, received_at_ms)
                          WHERE changed_at_ms IS NULL;
                          """,
                         connection,
                         transaction))
        {
            await backfill.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var notNull = new NpgsqlCommand(
                         $"""
                          ALTER TABLE {messages}
                          ALTER COLUMN "changed_at_ms" SET DEFAULT 0;
                          ALTER TABLE {messages}
                          ALTER COLUMN "changed_at_ms" SET NOT NULL;
                          """,
                         connection,
                         transaction))
        {
            await notNull.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var index = new NpgsqlCommand(
                         $"""
                          CREATE INDEX IF NOT EXISTS "ix_messages_conversation_changed"
                          ON {messages} (conversation_id, changed_at_ms, message_id);
                          """,
                         connection,
                         transaction))
        {
            await index.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var ledger = new NpgsqlCommand(
                         $"""
                          CREATE TABLE IF NOT EXISTS {mutationRequests} (
                              "actor_user_id" bigint NOT NULL,
                              "request_id" character varying(64) NOT NULL,
                              "operation" smallint NOT NULL,
                              "message_id" character varying(64) NOT NULL,
                              "payload_fingerprint" character varying(64) NOT NULL,
                              "succeeded" boolean NOT NULL,
                              "error_code" character varying(64) NULL,
                              "conversation_id" character varying(64) NULL,
                              "content" text NULL,
                              "edit_version" integer NULL,
                              "edited_at_ms" bigint NULL,
                              "recalled_at_ms" bigint NULL,
                              "created_at_ms" bigint NOT NULL,
                              PRIMARY KEY ("actor_user_id", "request_id")
                          );
                          """,
                         connection,
                         transaction))
        {
            await ledger.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
