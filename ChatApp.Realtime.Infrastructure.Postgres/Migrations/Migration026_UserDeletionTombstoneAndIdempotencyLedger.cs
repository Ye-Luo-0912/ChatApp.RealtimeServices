using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// LongTerm-1：用户删除 tombstone + 独立命令幂等性账本。
/// <para>
/// 解耦幂等性依据与 messages 行生命周期，防止 JetStream replay 在消息行被 retention GC
/// 或账号删除清理后将旧命令当作新消息重新写入（"复活"）。
/// </para>
/// <para>
/// Tombstone 在账号删除清理开始前写入（PK=user_id，幂等），Incoming Processor 检查
/// tombstone 后拒绝已注销用户的旧命令。账本记录 Created/Duplicate/Conflict 结果，
/// 保留期由 IdempotencyOptions 控制（不少于 JetStream MaxAge，启动时校验）。
/// </para>
/// </summary>
public sealed class Migration026_UserDeletionTombstoneAndIdempotencyLedger : IRealtimeSchemaMigration
{
    public int Version => 26;
    public string Name => "user_deletion_tombstone_and_idempotency_ledger";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var tombstones = schema.UserDeletionTombstonesTableSql;
        var ledger = schema.CommandIdempotencyLedgerTableSql;

        await using var tombstoneCommand = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {tombstones} (
                 "user_id" bigint NOT NULL PRIMARY KEY,
                 "deletion_event_id" character varying(128) NOT NULL,
                 "deleted_at_ms" bigint NOT NULL
             );
             """,
            connection,
            transaction);
        await tombstoneCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await using var tombstoneIndex = new NpgsqlCommand(
            $"""
             CREATE INDEX IF NOT EXISTS "ix_user_deletion_tombstones_deleted_at"
                 ON {tombstones} ("deleted_at_ms");
             """,
            connection,
            transaction);
        await tombstoneIndex.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await using var ledgerCommand = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {ledger} (
                 "sender_user_id" bigint NOT NULL,
                 "client_message_id" character varying(128) NOT NULL,
                 "command_id" character varying(64) NOT NULL,
                 "content_fingerprint" character varying(64) NOT NULL,
                 "result_kind" smallint NOT NULL,
                 "message_id" character varying(64) NULL,
                 "received_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("sender_user_id", "client_message_id")
             );
             """,
            connection,
            transaction);
        await ledgerCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await using var ledgerIndex = new NpgsqlCommand(
            $"""
             CREATE INDEX IF NOT EXISTS "ix_command_idempotency_ledger_received_at"
                 ON {ledger} ("received_at_ms");
             """,
            connection,
            transaction);
        await ledgerIndex.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
