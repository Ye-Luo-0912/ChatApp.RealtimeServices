using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 群操作幂等账本：与 message_mutation_requests 模式一致。
/// PK = (actor_user_id, request_id)，确保同一请求不会重复执行群变更。
/// 仅记录成功结果；失败不记录（重复失败无害）。
/// </summary>
public sealed class Migration024_GroupMutationRequests : IRealtimeSchemaMigration
{
    public int Version => 24;
    public string Name => "group_mutation_requests";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var table = $"{schema.QuotedSchema}.\"group_mutation_requests\"";

        await using var command = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {table} (
                 "actor_user_id" bigint NOT NULL,
                 "request_id" character varying(64) NOT NULL,
                 "operation" smallint NOT NULL,
                 "request_fingerprint" character varying(64) NOT NULL,
                 "conversation_id" character varying(64) NULL,
                 "succeeded" boolean NOT NULL,
                 "error_code" character varying(64) NULL,
                 "created_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("actor_user_id", "request_id")
             );
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
