using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 群成员 membership periods：记录每次入群/离群的时间段，用于精确控制历史可见性。
/// <para>
/// 重新入群后不能查看缺席期间的消息，需要依据此表过滤可见时间段。
/// 与 <c>conversation_members.left_at_ms</c> 并存：
/// <list type="bullet">
/// <item><c>left_at_ms</c> 仅记录最近一次离群时间，用于活跃成员判定与只读历史策略。</item>
/// <item><c>conversation_membership_periods</c> 记录完整入群/离群历史，支持多次往返场景的历史过滤。</item>
/// </list>
/// </para>
/// </summary>
public sealed class Migration035_MembershipPeriods : IRealtimeSchemaMigration
{
    public int Version => 35;
    public string Name => "membership_periods";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var table = schema.MembershipPeriodsTableSql;

        await using var command = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {table} (
                 "conversation_id" character varying(64) NOT NULL,
                 "user_id" bigint NOT NULL,
                 "joined_at_ms" bigint NOT NULL,
                 "left_at_ms" bigint NULL,
                 "left_reason" character varying(32) NULL,
                 PRIMARY KEY ("conversation_id", "user_id", "joined_at_ms")
             );

             CREATE INDEX IF NOT EXISTS "ix_membership_periods_user"
                 ON {table} ("user_id", "left_at_ms");
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
