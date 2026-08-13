namespace ChatApp.Realtime.IntegrationTests.Measurement;

/// <summary>
/// OUTBOX-DB-1 的 PG 级测量基石：快照数据模型。
/// <para>
/// 捕获一次测量窗口的 <c>pg_stat_statements</c>（按查询聚合的调用/耗时/行数/WAL）
/// 与 <c>pg_stat_wal</c>（全局 WAL 记录/字节/写入/同步），以及表级统计
/// （HOT 更新、死元组、live/dead tuple），用于把每消息成本拆成可归因的 SQL/WAL 项。
/// </para>
/// </summary>
public sealed record PostgresPerfSnapshot(
    IReadOnlyList<PgStatementStat> Statements,
    PgWalStat Wal,
    IReadOnlyList<PgTableStat> Tables);

/// <summary>单条被追踪语句的累计统计（来自 <c>pg_stat_statements</c>）。</summary>
public sealed record PgStatementStat(
    string QueryId,
    string Query,
    long Calls,
    double TotalExecMs,
    long Rows,
    long SharedBlksRead,
    long SharedBlksDirtied,
    long WalRecords,
    long WalFpi,
    long WalBytes);

/// <summary>全局 WAL 写入统计（来自 <c>pg_stat_wal</c>）。</summary>
public sealed record PgWalStat(
    long WalRecords,
    long WalFpi,
    long WalBytes,
    long WalWrite,
    long WalSync,
    double WalWriteTimeMs,
    double WalSyncTimeMs);

/// <summary>单表累计统计（来自 <c>pg_stat_user_tables</c>）。</summary>
public sealed record PgTableStat(
    string TableName,
    long Inserts,
    long Updates,
    long Deletes,
    long HotUpdates,
    long LiveTuples,
    long DeadTuples);

/// <summary>两次快照之间的增量，用于 A/B 前后对比。</summary>
public sealed record PostgresPerfDiff(
    IReadOnlyList<PgStatementDiff> Statements,
    PgWalStat Wal,
    IReadOnlyList<PgTableDiff> Tables);

/// <summary>单条语句的窗口内增量。</summary>
public sealed record PgStatementDiff(
    string QueryId,
    string Query,
    long Calls,
    double TotalExecMs,
    long Rows,
    long SharedBlksRead,
    long SharedBlksDirtied,
    long WalRecords,
    long WalFpi,
    long WalBytes);

/// <summary>单表的窗口内增量。</summary>
public sealed record PgTableDiff(
    string TableName,
    long Inserts,
    long Updates,
    long Deletes,
    long HotUpdates,
    long DeadTuples);

/// <summary>
/// 对两个快照求增量。语句按 <c>queryid</c> 对齐；表按表名对齐。
/// </summary>
public static class PostgresPerfDiffCalculator
{
    public static PostgresPerfDiff Diff(
        PostgresPerfSnapshot before,
        PostgresPerfSnapshot after)
    {
        var statementDiffs = new List<PgStatementDiff>();
        var beforeStmts = before.Statements.ToDictionary(s => s.QueryId);
        foreach (var s in after.Statements)
        {
            if (!beforeStmts.TryGetValue(s.QueryId, out var b))
            {
                b = PostgresPerfZero.Statement(s.QueryId, s.Query);
            }

            var d = new PgStatementDiff(
                s.QueryId,
                s.Query,
                s.Calls - b.Calls,
                s.TotalExecMs - b.TotalExecMs,
                s.Rows - b.Rows,
                s.SharedBlksRead - b.SharedBlksRead,
                s.SharedBlksDirtied - b.SharedBlksDirtied,
                s.WalRecords - b.WalRecords,
                s.WalFpi - b.WalFpi,
                s.WalBytes - b.WalBytes);
            if (d.Calls > 0)
            {
                statementDiffs.Add(d);
            }
        }

        var wal = new PgWalStat(
            after.Wal.WalRecords - before.Wal.WalRecords,
            after.Wal.WalFpi - before.Wal.WalFpi,
            after.Wal.WalBytes - before.Wal.WalBytes,
            after.Wal.WalWrite - before.Wal.WalWrite,
            after.Wal.WalSync - before.Wal.WalSync,
            after.Wal.WalWriteTimeMs - before.Wal.WalWriteTimeMs,
            after.Wal.WalSyncTimeMs - before.Wal.WalSyncTimeMs);

        var tableDiffs = new List<PgTableDiff>();
        var beforeTables = before.Tables.ToDictionary(t => t.TableName);
        foreach (var t in after.Tables)
        {
            beforeTables.TryGetValue(t.TableName, out var b);
            tableDiffs.Add(new PgTableDiff(
                t.TableName,
                t.Inserts - (b?.Inserts ?? 0),
                t.Updates - (b?.Updates ?? 0),
                t.Deletes - (b?.Deletes ?? 0),
                t.HotUpdates - (b?.HotUpdates ?? 0),
                t.DeadTuples - (b?.DeadTuples ?? 0)));
        }

        return new PostgresPerfDiff(statementDiffs.OrderByDescending(d => d.TotalExecMs).ToList(), wal, tableDiffs);
    }
}

/// <summary>用于求差时补齐缺口的零值基线。</summary>
internal static class PostgresPerfZero
{
    public static PgStatementStat Statement(string queryId, string query) =>
        new(queryId, query, 0, 0, 0, 0, 0, 0, 0, 0);
}