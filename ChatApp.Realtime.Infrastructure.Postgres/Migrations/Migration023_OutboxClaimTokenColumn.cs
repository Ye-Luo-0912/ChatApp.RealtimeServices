using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Outbox 增加 claim_token 列，用于 lease 的不可复用所有权凭证。
/// 解决同一实例标识在 lease 过期并重新领取后，旧任务误完成新 lease 的问题。
/// </summary>
public sealed class Migration023_OutboxClaimTokenColumn : IRealtimeSchemaMigration
{
    public int Version => 23;
    public string Name => "outbox_claim_token_column";

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
             ADD COLUMN IF NOT EXISTS "claim_token" text NULL;
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
