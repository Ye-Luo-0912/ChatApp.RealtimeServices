using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Perf-4：Outbox 表持久化预序列化的 UTF-8 payload 字节，消除 Publisher 的 JSON reserialize。
/// <para>
/// 新增列：
/// - <c>payload_utf8</c>：与 <c>payload_json</c> 内容等价的 UTF-8 字节，Publisher 直接发送避免重新序列化。
/// </para>
/// <para>
/// 保留 <c>payload_json</c> 列以兼容旧代码与查询。<c>payload_utf8</c> 可为 NULL（旧数据），
/// Publisher 在 NULL 时回退到 <c>payload_json</c> 反序列化 + 重新序列化路径。
/// 不需要索引：<c>payload_utf8</c> 不用于查询。
/// </para>
/// </summary>
public sealed class Migration034_OutboxPayloadUtf8 : IRealtimeSchemaMigration
{
    public int Version => 34;
    public string Name => "outbox_payload_utf8";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var outbox = schema.OutboxTableSql;

        await using var command = new NpgsqlCommand(
            $"""
            ALTER TABLE {outbox}
                ADD COLUMN IF NOT EXISTS "payload_utf8" bytea NULL;
            """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
