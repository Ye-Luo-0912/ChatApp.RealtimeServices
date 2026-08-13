using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// OUTBOX-DB-1：将 outbox 表 fillfactor 进一步从 75 调低到 50，让 delete-on-complete 排水的
/// claim 单次 HOT 更新命中率逼近 100%。
/// <para>
/// 排水热路径每行两次写：claim（locked_by/claim_token/locked_until_ms/attempt_count，全为非索引
/// 列，理论上可 HOT）与 complete（delete-on-complete 模式即 DELETE，不触发索引维护）。HOT 要求
/// 新版本落在旧版本同页，且页内须同时容纳旧+新版本；按页填充率模型
/// HOT 命中率 ≈ (100 − fillfactor) / fillfactor。A/B 实测（语句级 wal_bytes 归因，同容器仅改
/// fillfactor，两窗口前均 TRUNCATE 从空表起始）：
/// </para>
/// <list type="bullet">
/// <item>fillfactor=75：claim 917 + complete 62 = 979 WAL 字节/消息，outbox HOT 命中率 30%</item>
/// <item>fillfactor=50：claim 359 + complete 66 = 425 WAL 字节/消息（排水合计 -57%），outbox HOT 命中率 87%</item>
/// </list>
/// <para>
/// 代价是每页行数减半、表空间占用上升（outbox 为待排水的短期表，行很快被 claim/complete/删除，
/// 页空间随即释放，净成本可接受）；fillfactor 只影响改变之后新插入行所在的页，已有数据不受影响，
/// 因此该迁移不锁表、可安全滚动生效。
/// </para>
/// </summary>
public sealed class Migration069_OutboxHotDrainFillfactor50 : IRealtimeSchemaMigration
{
    public int Version => 69;
    public string Name => "outbox_hot_drain_fillfactor_50";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"ALTER TABLE {schema.OutboxTableSql} SET (fillfactor = 50);",
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
