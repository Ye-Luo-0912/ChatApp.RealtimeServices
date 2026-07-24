using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 消息表情反应表（message_id, user_id, emoji）唯一；反应变更会 bump messages.changed_at_ms。
/// </summary>
public sealed class Migration018_MessageReactions : IRealtimeSchemaMigration
{
    public int Version => 18;
    public string Name => "message_reactions";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var reactions = schema.MessageReactionsTableSql;

        await using (var create = new NpgsqlCommand(
                         $"""
                          CREATE TABLE IF NOT EXISTS {reactions} (
                              "message_id" character varying(64) NOT NULL,
                              "user_id" bigint NOT NULL,
                              "emoji" character varying(32) NOT NULL,
                              "created_at_ms" bigint NOT NULL,
                              PRIMARY KEY ("message_id", "user_id", "emoji"),
                              CONSTRAINT "ck_message_reactions_user_positive" CHECK ("user_id" > 0),
                              CONSTRAINT "ck_message_reactions_emoji_nonempty" CHECK (char_length(btrim("emoji")) > 0),
                              CONSTRAINT "ck_message_reactions_created_positive" CHECK ("created_at_ms" > 0)
                          );
                          """,
                         connection,
                         transaction))
        {
            await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var index = new NpgsqlCommand(
                         $"""
                          CREATE INDEX IF NOT EXISTS "ix_message_reactions_message"
                          ON {reactions} (message_id, created_at_ms);
                          """,
                         connection,
                         transaction))
        {
            await index.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
