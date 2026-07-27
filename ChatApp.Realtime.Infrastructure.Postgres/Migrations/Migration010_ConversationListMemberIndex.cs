using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 会话列表成员索引：为 members 增加 last_message_at_ms，并重建含 tip 的列表索引。
/// </summary>
/// <remarks>
/// Dual-write tip 已存在；读侧 COALESCE(members.last_message_at_ms, conversations.last_message_at_ms)。
/// RequiresTransaction=false：CREATE/DROP INDEX CONCURRENTLY 不可在事务内；
/// 回填按 conversation_id 小事务分批，并写 schema_migration_checkpoints。
/// </remarks>
public sealed class Migration010_ConversationListMemberIndex : IRealtimeSchemaMigration
{
    public const int DefaultBatchSize = 5_000;
    private const string AddColumnPhase = "add_column";
    private const string BackfillPhase = "backfill_last_message_at_ms";
    private const string IndexPhase = "rebuild_index";

    public int Version => 10;
    public string Name => "conversation_list_member_index";
    public bool RequiresTransaction => false;

    public int BatchSize { get; init; } = DefaultBatchSize;

    /// <summary>测试用：限制回填批次数；达到后抛 Deferred，不记为已应用。</summary>
    public int? MaxBatches { get; init; }

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var members = schema.ConversationMembersTableSql;
        var conversations = schema.ConversationsTableSql;
        var quotedSchema = schema.QuotedSchema;
        var checkpoints = schema.SchemaMigrationCheckpointsTableSql;
        var batchSize = Math.Clamp(BatchSize, 100, 50_000);

        await EnsureCheckpointTableAsync(connection, checkpoints, cancellationToken)
            .ConfigureAwait(false);

        if (!await IsPhaseCompleteAsync(connection, checkpoints, AddColumnPhase, cancellationToken)
                .ConfigureAwait(false))
        {
            await using var add = new NpgsqlCommand(
                $"""
                 ALTER TABLE {members}
                     ADD COLUMN IF NOT EXISTS "last_message_at_ms" bigint NULL;
                 """,
                connection);
            await add.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await MarkPhaseCompleteAsync(connection, checkpoints, AddColumnPhase, cancellationToken)
                .ConfigureAwait(false);
        }

        if (!await IsPhaseCompleteAsync(connection, checkpoints, BackfillPhase, cancellationToken)
                .ConfigureAwait(false))
        {
            var batchesProcessed = 0;
            var afterConversationId = await ReadBackfillCursorAsync(
                    connection,
                    checkpoints,
                    cancellationToken)
                .ConfigureAwait(false);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (MaxBatches is int max && batchesProcessed >= max)
                {
                    throw new RealtimeMigrationDeferredException(
                        Version,
                        Name,
                        $"回填已达 MaxBatches={max}，保留检查点以便续跑。");
                }

                await using var batchTxn = await connection
                    .BeginTransactionAsync(cancellationToken)
                    .ConfigureAwait(false);

                await using var select = new NpgsqlCommand(
                    $"""
                     SELECT conversation_id
                     FROM {conversations}
                     WHERE (@after_id = '' OR conversation_id > @after_id)
                     ORDER BY conversation_id
                     LIMIT @batch_size
                     FOR UPDATE SKIP LOCKED;
                     """,
                    connection,
                    batchTxn);
                select.Parameters.AddWithValue("after_id", afterConversationId);
                select.Parameters.AddWithValue("batch_size", batchSize);

                var ids = new List<string>(batchSize);
                await using (var reader = await select.ExecuteReaderAsync(cancellationToken)
                                 .ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        ids.Add(reader.GetString(0));
                }

                if (ids.Count == 0)
                {
                    await batchTxn.CommitAsync(cancellationToken).ConfigureAwait(false);
                    await MarkPhaseCompleteAsync(connection, checkpoints, BackfillPhase, cancellationToken)
                        .ConfigureAwait(false);
                    break;
                }

                await using var update = new NpgsqlCommand(
                    $"""
                     UPDATE {members} AS m
                     SET last_message_at_ms = c.last_message_at_ms
                     FROM {conversations} AS c
                     WHERE c.conversation_id = m.conversation_id
                       AND c.conversation_id = ANY(@ids)
                       AND m.last_message_at_ms IS DISTINCT FROM c.last_message_at_ms;
                     """,
                    connection,
                    batchTxn);
                update.Parameters.AddWithValue("ids", ids.ToArray());
                await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                afterConversationId = ids[^1];
                await WriteBackfillCursorAsync(
                        connection,
                        batchTxn,
                        checkpoints,
                        afterConversationId,
                        cancellationToken)
                    .ConfigureAwait(false);

                await batchTxn.CommitAsync(cancellationToken).ConfigureAwait(false);
                batchesProcessed++;
            }
        }

        if (!await IsPhaseCompleteAsync(connection, checkpoints, IndexPhase, cancellationToken)
                .ConfigureAwait(false))
        {
            // LongTerm-3：通过 ConcurrentIndexHelper 检查 indisvalid，INVALID 时自动 DROP 后重建。
            await ConcurrentIndexHelper.EnsureValidAsync(
                    connection,
                    quotedSchema,
                    schema.Schema,
                    "ix_conversation_members_user_pinned_list",
                    $"""
                     CREATE INDEX CONCURRENTLY "ix_conversation_members_user_pinned_list"
                         ON {members} (
                             "user_id",
                             "is_pinned" DESC,
                             "pinned_at_ms" DESC NULLS LAST,
                             "last_message_at_ms" DESC NULLS LAST,
                             "conversation_id" DESC
                         );
                     """,
                    cancellationToken)
                .ConfigureAwait(false);

            await MarkPhaseCompleteAsync(connection, checkpoints, IndexPhase, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static async Task EnsureCheckpointTableAsync(
        NpgsqlConnection connection,
        string checkpointsTable,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {checkpointsTable} (
                 "migration_version" integer NOT NULL,
                 "phase" character varying(64) NOT NULL,
                 "checkpoint_key" character varying(128) NOT NULL DEFAULT '',
                 "checkpoint_value" text NULL,
                 "updated_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("migration_version", "phase", "checkpoint_key")
             );
             """,
            connection);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task<bool> IsPhaseCompleteAsync(
        NpgsqlConnection connection,
        string checkpointsTable,
        string phase,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT checkpoint_value
             FROM {checkpointsTable}
             WHERE migration_version = 10
               AND phase = @phase
               AND checkpoint_key = ''
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("phase", phase);
        var value = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return string.Equals(value as string, "done", StringComparison.Ordinal);
    }

    private static async Task MarkPhaseCompleteAsync(
        NpgsqlConnection connection,
        string checkpointsTable,
        string phase,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {checkpointsTable} (
                 migration_version, phase, checkpoint_key, checkpoint_value, updated_at_ms)
             VALUES (10, @phase, '', 'done', @now)
             ON CONFLICT (migration_version, phase, checkpoint_key) DO UPDATE SET
                 checkpoint_value = EXCLUDED.checkpoint_value,
                 updated_at_ms = EXCLUDED.updated_at_ms;
             """,
            connection);
        command.Parameters.AddWithValue("phase", phase);
        command.Parameters.AddWithValue("now", now);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task<string> ReadBackfillCursorAsync(
        NpgsqlConnection connection,
        string checkpointsTable,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT checkpoint_value
             FROM {checkpointsTable}
             WHERE migration_version = 10
               AND phase = @phase
               AND checkpoint_key = ''
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("phase", BackfillPhase);
        var value = await command.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        return string.IsNullOrWhiteSpace(value) || string.Equals(value, "done", StringComparison.Ordinal)
            ? string.Empty
            : value;
    }

    private static async Task WriteBackfillCursorAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string checkpointsTable,
        string conversationId,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {checkpointsTable} (
                 migration_version, phase, checkpoint_key, checkpoint_value, updated_at_ms)
             VALUES (10, @phase, '', @cursor, @now)
             ON CONFLICT (migration_version, phase, checkpoint_key) DO UPDATE SET
                 checkpoint_value = EXCLUDED.checkpoint_value,
                 updated_at_ms = EXCLUDED.updated_at_ms;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("phase", BackfillPhase);
        command.Parameters.AddWithValue("cursor", conversationId);
        command.Parameters.AddWithValue("now", now);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
