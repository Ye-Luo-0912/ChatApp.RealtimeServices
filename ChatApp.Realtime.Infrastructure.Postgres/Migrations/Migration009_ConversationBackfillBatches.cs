using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Online conversation backfill (batched, idempotent, resumable).
/// RequiresTransaction=false: per-batch commit + checkpoints; keyset + FOR UPDATE SKIP LOCKED.
/// </summary>
public sealed class Migration009_ConversationBackfillBatches : IRealtimeSchemaMigration
{
    public const int DefaultBatchSize = 5_000;
    private const string MessagesPhase = "messages_conversation_id";
    private const string ConversationsPhase = "conversations";
    private const string MembersSendersPhase = "members_senders";
    private const string MembersReceiversPhase = "members_receivers";

    public int Version => 9;
    public string Name => "conversation_backfill_batches";
    public bool RequiresTransaction => false;

    public int BatchSize { get; init; } = DefaultBatchSize;

    /// <summary>Test hook: stop after N batches and throw Deferred (version not recorded).</summary>
    public int? MaxBatches { get; init; }

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var messages = schema.MessagesTableSql;
        var conversations = schema.ConversationsTableSql;
        var members = schema.ConversationMembersTableSql;
        var checkpoints = schema.SchemaMigrationCheckpointsTableSql;
        var batchSize = Math.Clamp(BatchSize, 1, 50_000);

        await EnsureCheckpointTableAsync(connection, checkpoints, cancellationToken)
            .ConfigureAwait(false);

        if (!await IsPhaseCompleteAsync(connection, checkpoints, ConversationsPhase, cancellationToken)
                .ConfigureAwait(false))
        {
            var batchesProcessed = 0;
            var (cursorAt, cursorId) = await ReadMessageCursorAsync(
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
                        $"MaxBatches={max}; checkpoint retained for resume.");
                }

                await using var batchTxn = await connection
                    .BeginTransactionAsync(cancellationToken)
                    .ConfigureAwait(false);

                await using var select = new NpgsqlCommand(
                    $"""
                     SELECT message_id, received_at_ms
                     FROM {messages}
                     WHERE conversation_id IS NULL
                       AND sender_user_id > 0
                       AND receiver_user_id > 0
                       AND sender_user_id <> receiver_user_id
                       AND (
                           @cursor_at = 0
                           OR received_at_ms > @cursor_at
                           OR (received_at_ms = @cursor_at AND message_id > @cursor_id)
                       )
                     ORDER BY received_at_ms, message_id
                     LIMIT @batch_size
                     FOR UPDATE SKIP LOCKED;
                     """,
                    connection,
                    batchTxn);
                select.Parameters.AddWithValue("cursor_at", cursorAt);
                select.Parameters.AddWithValue("cursor_id", cursorId ?? string.Empty);
                select.Parameters.AddWithValue("batch_size", batchSize);

                string? lastMessageId = null;
                long lastReceivedAtMs = 0;
                var ids = new List<string>(batchSize);
                await using (var reader = await select
                                 .ExecuteReaderAsync(cancellationToken)
                                 .ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        lastMessageId = reader.GetString(0);
                        lastReceivedAtMs = reader.GetInt64(1);
                        ids.Add(lastMessageId);
                    }
                }

                if (ids.Count == 0)
                {
                    await batchTxn.CommitAsync(cancellationToken).ConfigureAwait(false);
                    break;
                }

                await using var update = new NpgsqlCommand(
                    $"""
                     UPDATE {messages} AS m
                     SET conversation_id = 'dm:' || LEAST(m.sender_user_id, m.receiver_user_id)::text
                         || ':' || GREATEST(m.sender_user_id, m.receiver_user_id)::text
                     WHERE m.message_id = ANY(@message_ids);
                     """,
                    connection,
                    batchTxn);
                update.Parameters.AddWithValue("message_ids", ids.ToArray());
                await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                await WriteCheckpointAsync(
                        connection,
                        batchTxn,
                        checkpoints,
                        MessagesPhase,
                        checkpointValue: $"{lastReceivedAtMs}|{lastMessageId}",
                        cancellationToken)
                    .ConfigureAwait(false);

                await batchTxn.CommitAsync(cancellationToken).ConfigureAwait(false);
                cursorAt = lastReceivedAtMs;
                cursorId = lastMessageId;
                batchesProcessed++;
            }

            if (!await RunPhaseOnceAsync(
                        connection,
                        checkpoints,
                        ConversationsPhase,
                        $"""
                         INSERT INTO {conversations} (
                             conversation_id, type, created_at_ms, updated_at_ms,
                             last_message_id, last_message_preview, last_message_at_ms, last_sender_user_id
                         )
                         SELECT
                             latest.conversation_id,
                             1,
                             latest.received_at_ms,
                             latest.received_at_ms,
                             latest.message_id,
                             LEFT(latest.content, 256),
                             latest.received_at_ms,
                             latest.sender_user_id
                         FROM (
                             SELECT DISTINCT ON (conversation_id)
                                 conversation_id,
                                 message_id,
                                 content,
                                 received_at_ms,
                                 sender_user_id
                             FROM {messages}
                             WHERE conversation_id IS NOT NULL
                             ORDER BY conversation_id, received_at_ms DESC, message_id DESC
                         ) AS latest
                         ON CONFLICT (conversation_id) DO NOTHING;
                         """,
                        cancellationToken)
                    .ConfigureAwait(false))
            {
                return;
            }
        }

        if (!await RunPhaseOnceAsync(
                    connection,
                    checkpoints,
                    MembersSendersPhase,
                    $"""
                     INSERT INTO {members} (conversation_id, user_id, peer_user_id, joined_at_ms)
                     SELECT DISTINCT conversation_id, sender_user_id, receiver_user_id, MIN(received_at_ms)
                     FROM {messages}
                     WHERE conversation_id IS NOT NULL
                     GROUP BY conversation_id, sender_user_id, receiver_user_id
                     ON CONFLICT (conversation_id, user_id) DO NOTHING;
                     """,
                    cancellationToken)
                .ConfigureAwait(false))
        {
            return;
        }

        await RunPhaseOnceAsync(
                connection,
                checkpoints,
                MembersReceiversPhase,
                $"""
                 INSERT INTO {members} (conversation_id, user_id, peer_user_id, joined_at_ms)
                 SELECT DISTINCT conversation_id, receiver_user_id, sender_user_id, MIN(received_at_ms)
                 FROM {messages}
                 WHERE conversation_id IS NOT NULL
                 GROUP BY conversation_id, receiver_user_id, sender_user_id
                 ON CONFLICT (conversation_id, user_id) DO NOTHING;
                 """,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task EnsureCheckpointTableAsync(
        NpgsqlConnection connection,
        string checkpointsTable,
        CancellationToken cancellationToken)
    {
        await using var create = new NpgsqlCommand(
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
        await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<bool> IsPhaseCompleteAsync(
        NpgsqlConnection connection,
        string checkpointsTable,
        string phase,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT checkpoint_value
             FROM {checkpointsTable}
             WHERE migration_version = @version
               AND phase = @phase
               AND checkpoint_key = ''
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("version", 9);
        command.Parameters.AddWithValue("phase", phase);
        var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return string.Equals(value as string, "done", StringComparison.Ordinal);
    }

    private static async Task<(long ReceivedAtMs, string? MessageId)> ReadMessageCursorAsync(
        NpgsqlConnection connection,
        string checkpointsTable,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT checkpoint_value
             FROM {checkpointsTable}
             WHERE migration_version = @version
               AND phase = @phase
               AND checkpoint_key = ''
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("version", 9);
        command.Parameters.AddWithValue("phase", MessagesPhase);
        var raw = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
        if (string.IsNullOrWhiteSpace(raw))
            return (0, string.Empty);

        var separator = raw.IndexOf('|', StringComparison.Ordinal);
        if (separator <= 0 || separator >= raw.Length - 1)
            return (0, string.Empty);

        if (!long.TryParse(raw[..separator], out var at))
            return (0, string.Empty);

        return (at, raw[(separator + 1)..]);
    }

    private static async Task WriteCheckpointAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string checkpointsTable,
        string phase,
        string checkpointValue,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {checkpointsTable} (
                 migration_version, phase, checkpoint_key, checkpoint_value, updated_at_ms
             ) VALUES (
                 @version, @phase, '', @value, @updated_at_ms
             )
             ON CONFLICT (migration_version, phase, checkpoint_key)
             DO UPDATE SET
                 checkpoint_value = EXCLUDED.checkpoint_value,
                 updated_at_ms = EXCLUDED.updated_at_ms;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("version", 9);
        command.Parameters.AddWithValue("phase", phase);
        command.Parameters.AddWithValue("value", checkpointValue);
        command.Parameters.AddWithValue(
            "updated_at_ms",
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<bool> RunPhaseOnceAsync(
        NpgsqlConnection connection,
        string checkpointsTable,
        string phase,
        string sql,
        CancellationToken cancellationToken)
    {
        if (await IsPhaseCompleteAsync(connection, checkpointsTable, phase, cancellationToken)
                .ConfigureAwait(false))
        {
            return true;
        }

        await using var txn = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection, txn);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        await WriteCheckpointAsync(
                connection,
                txn,
                checkpointsTable,
                phase,
                checkpointValue: "done",
                cancellationToken)
            .ConfigureAwait(false);
        await txn.CommitAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }
}
