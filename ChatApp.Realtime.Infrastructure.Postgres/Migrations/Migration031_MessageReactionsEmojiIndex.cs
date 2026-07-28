using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 为 message_reactions 添加 (message_id, emoji) 复合索引。
/// 现有 PK (message_id, user_id, emoji) 只能按 message_id 前缀扫描，
/// EmojiExistsOnMessageAsync / CountEmojiAsync 按 (message_id, emoji) 过滤时需扫描该消息下所有用户行。
/// 新索引直接命中 (message_id, emoji) 组合，消除 emoji 过滤的行扫描。
/// </summary>
public sealed class Migration031_MessageReactionsEmojiIndex : IRealtimeSchemaMigration
{
    public int Version => 31;
    public string Name => "message_reactions_emoji_index";
    public bool RequiresTransaction => false;

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
            CREATE INDEX IF NOT EXISTS "ix_message_reactions_message_emoji"
            ON {schema.MessageReactionsTableSql} (message_id, emoji);
            """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
