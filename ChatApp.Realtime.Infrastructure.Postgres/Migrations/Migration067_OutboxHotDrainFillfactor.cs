using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// OUTBOX-DB-1：将 outbox 表 fillfactor 从 90 调低到 75，为 claim/complete 的 HOT 更新预留页内空间。
/// <para>
/// 排水热路径对每行连续两次 UPDATE：claim（locked_by/claim_token/locked_until_ms/attempt_count，
/// 全为非索引列，理论上可 HOT）与 complete（published_at_ms 是 <c>ix_outbox_pending</c>
/// 部分索引谓词列，必然 non-HOT）。A/B 实测（语句级 wal_bytes 归因，同容器仅改 fillfactor）：
/// </para>
/// <list type="bullet">
/// <item>fillfactor=90：claim 1,094 + complete 953 = 2,047 WAL 字节/消息</item>
/// <item>fillfactor=75：claim 932 + complete 773 = 1,705 WAL 字节/消息（-16.7%）</item>
/// </list>
/// <para>
/// 调低填充率给刚插入后很快被 claim/complete 的 tuple 留出版内版本链空间，提高 HOT 命中、
/// 减少主表行跨页重写。fillfactor 只影响改变之后新插入行所在的页，已有数据不受影响，
/// 因此该迁移不锁表、可安全滚动生效。
/// </para>
/// </summary>
public sealed class Migration067_OutboxHotDrainFillfactor : IRealtimeSchemaMigration
{
    public int Version => 67;
    public string Name => "outbox_hot_drain_fillfactor";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"ALTER TABLE {schema.OutboxTableSql} SET (fillfactor = 75);",
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}