using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 消息内容指纹：同 (sender_user_id, client_message_id) 下区分真幂等重放与内容冲突�?
/// 历史行允�?NULL；读取冲突路径时由应用按 receiver+content 计算�?
/// </summary>
public sealed class Migration004_MessageContentFingerprint : IRealtimeSchemaMigration
{
    public int Version => 4;
    public string Name => "message_content_fingerprint";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var messages = schema.MessagesTableSql;
        await using var command = new NpgsqlCommand(
            $"ALTER TABLE {messages} ADD COLUMN IF NOT EXISTS \"content_fingerprint\" character varying(64) NULL;",
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
