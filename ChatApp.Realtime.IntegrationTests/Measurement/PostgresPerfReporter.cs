using System.Text;

namespace ChatApp.Realtime.IntegrationTests.Measurement;

/// <summary>
/// OUTBOX-DB-1：把 <see cref="PostgresPerfDiff"/> 渲染成可读的 markdown 报告。
/// 报告按"每消息成本"拆分各条 SQL 的调用/耗时/行数/共享块读取/WAL 字节，并汇总
/// 全局 WAL 与表级 HOT/死元组变化，供 A/B 前后对照。
/// </summary>
public static class PostgresPerfReporter
{
    public static string Render(
        PostgresPerfDiff diff,
        int messageCount,
        string phaseTitle)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"## {phaseTitle}（{messageCount} 条近热路径消息）");
        sb.AppendLine();

        // 每消息 WAL 汇总
        var walPerMsg = messageCount > 0 ? diff.Wal.WalBytes / (double)messageCount : 0;
        var recPerMsg = messageCount > 0 ? diff.Wal.WalRecords / (double)messageCount : 0;
        var fpiPerMsg = messageCount > 0 ? diff.Wal.WalFpi / (double)messageCount : 0;
        var writePerMsg = messageCount > 0 ? diff.Wal.WalWrite / (double)messageCount : 0;
        var syncPerMsg = messageCount > 0 ? diff.Wal.WalSync / (double)messageCount : 0;
        sb.AppendLine("### 每消息成本汇总（WAL）");
        sb.AppendLine();
        sb.AppendLine("| 指标 | 窗口总量 | 每消息 |");
        sb.AppendLine("|---|---|---|");
        sb.AppendLine($"| WAL 字节 | {diff.Wal.WalBytes:N0} | {walPerMsg:N0} |");
        sb.AppendLine($"| WAL 记录 | {diff.Wal.WalRecords:N0} | {recPerMsg:N1} |");
        sb.AppendLine($"| WAL FPI | {diff.Wal.WalFpi:N0} | {fpiPerMsg:N2} |");
        sb.AppendLine($"| WAL write | {diff.Wal.WalWrite:N0} | {writePerMsg:N3} |");
        sb.AppendLine($"| WAL sync | {diff.Wal.WalSync:N0} | {syncPerMsg:N3} |");
        sb.AppendLine();

        // Top SQL by total time
        sb.AppendLine("### Top SQL（按执行耗时降序，窗口增量）");
        sb.AppendLine();
        sb.AppendLine("| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |");
        sb.AppendLine("|---|---|---|---|---|---|---|---|---|---|");
        foreach (var s in diff.Statements.Take(30))
        {
            var sql = s.Query.Length > 90 ? s.Query[..90] + "…" : s.Query;
            sb.AppendLine($"| {s.QueryId} | {s.Calls:N0} | {s.TotalExecMs:N1} | {s.Rows:N0} | {s.SharedBlksRead:N0} | {s.SharedBlksDirtied:N0} | {s.WalRecords:N0} | {s.WalFpi:N0} | {s.WalBytes:N0} | `{Sanitize(sql)}` |");
        }

        sb.AppendLine();

        // Top SQL by WAL bytes
        sb.AppendLine("### Top SQL（按 WAL 字节降序，窗口增量）");
        sb.AppendLine();
        sb.AppendLine("| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |");
        sb.AppendLine("|---|---|---|---|---|---|");
        foreach (var s in diff.Statements.OrderByDescending(s => s.WalBytes).Take(15))
        {
            var sql = s.Query.Length > 90 ? s.Query[..90] + "…" : s.Query;
            sb.AppendLine($"| {s.QueryId} | {s.Calls:N0} | {s.WalBytes:N0} | {s.WalRecords:N0} | {s.WalFpi:N0} | `{Sanitize(sql)}` |");
        }

        sb.AppendLine();

        // 表级统计
        sb.AppendLine("### 表级统计（窗口增量）");
        sb.AppendLine();
        sb.AppendLine("| table | inserts | updates | deletes | hot_updates | dead_tuples |");
        sb.AppendLine("|---|---|---|---|---|---|");
        foreach (var t in diff.Tables)
        {
            sb.AppendLine($"| {t.TableName} | {t.Inserts:N0} | {t.Updates:N0} | {t.Deletes:N0} | {t.HotUpdates:N0} | {t.DeadTuples:N0} |");
        }

        // HOT 命中率聚焦（排水热路径涉及的更新表）。
        var hotFocus = diff.Tables
            .Where(t => t.Updates > 0)
            .OrderByDescending(t => t.Updates)
            .Take(5)
            .ToList();
        if (hotFocus.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("> HOT 命中率（hot_updates / updates）：" +
                          string.Join("；", hotFocus.Select(t =>
                              $"{t.TableName} {t.HotUpdates:N0}/{t.Updates:N0}（{(t.Updates > 0 ? t.HotUpdates * 100.0 / t.Updates : 0):N0}%）")) +
                          "。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。");
        }

        sb.AppendLine();
        return sb.ToString();
    }

    private static string Sanitize(string sql) =>
        sql.Replace("|", "\\|", StringComparison.Ordinal).ReplaceLineEndings(" ");
}