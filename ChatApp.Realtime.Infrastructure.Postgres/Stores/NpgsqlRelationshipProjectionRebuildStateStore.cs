using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRelationshipProjectionRebuildStateStore(
    RealtimeDatabaseClient databaseClient,
    RealtimeDatabaseSchema schema) : IRelationshipProjectionRebuildStateStore
{
    public async Task<RelationshipProjectionRebuildLease?> TryAcquireAsync(
        string leaseOwner,
        TimeSpan leaseDuration,
        CancellationToken ct = default)
    {
        ValidateOwnerAndDuration(leaseOwner, leaseDuration);
        var claimToken = Guid.NewGuid().ToString("N");
        var leaseMs = checked((long)leaseDuration.TotalMilliseconds);
        var sql = schema.GetOrAddCommandText(
            "relationship-projection-rebuild-acquire",
            static current => $"""
                WITH clock AS (
                    SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
                )
                UPDATE {current.RelationshipProjectionRebuildStateTableSql} AS state
                SET "lease_owner" = @lease_owner,
                    "claim_token" = @claim_token,
                    "locked_until_ms" = clock.now_ms + @lease_ms,
                    "updated_at_ms" = clock.now_ms
                FROM clock
                WHERE "id" = 1
                  AND "next_attempt_at_ms" <= clock.now_ms
                  AND ("locked_until_ms" IS NULL OR "locked_until_ms" < clock.now_ms)
                RETURNING "after_owner_user_id", "after_list_type", "pass_number",
                          "pass_changed", "stable_passes";
                """);

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("lease_owner", NpgsqlDbType.Varchar, leaseOwner);
        command.Parameters.AddWithValue("claim_token", NpgsqlDbType.Varchar, claimToken);
        command.Parameters.AddWithValue("lease_ms", NpgsqlDbType.Bigint, leaseMs);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        var afterOwner = reader.IsDBNull(0) ? (long?)null : reader.GetInt64(0);
        var afterList = reader.IsDBNull(1)
            ? (RelationshipProjectionListType?)null
            : (RelationshipProjectionListType)reader.GetInt16(1);
        return new RelationshipProjectionRebuildLease(
            leaseOwner,
            claimToken,
            afterOwner,
            afterList,
            reader.GetInt64(2),
            reader.GetBoolean(3),
            reader.GetInt32(4));
    }

    public Task<bool> RenewAsync(
        RelationshipProjectionRebuildLease lease,
        TimeSpan leaseDuration,
        CancellationToken ct = default) =>
        UpdateLeaseAsync(
            lease,
            leaseDuration,
            static current => $"""
                WITH clock AS (
                    SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
                )
                UPDATE {current.RelationshipProjectionRebuildStateTableSql} AS state
                SET "locked_until_ms" = clock.now_ms + @lease_ms,
                    "updated_at_ms" = clock.now_ms
                FROM clock
                WHERE "id" = 1
                  AND "lease_owner" = @lease_owner
                  AND "claim_token" = @claim_token
                  AND "locked_until_ms" >= clock.now_ms;
                """,
            "relationship-projection-rebuild-renew",
            null,
            ct);

    public Task<bool> CommitPageAsync(
        RelationshipProjectionRebuildLease lease,
        long nextOwnerUserId,
        RelationshipProjectionListType nextListType,
        bool pageChanged,
        TimeSpan leaseDuration,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(nextOwnerUserId);
        if (!Enum.IsDefined(nextListType))
            throw new ArgumentOutOfRangeException(nameof(nextListType));
        return UpdateLeaseAsync(
            lease,
            leaseDuration,
            static current => $"""
                WITH clock AS (
                    SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
                )
                UPDATE {current.RelationshipProjectionRebuildStateTableSql} AS state
                SET "after_owner_user_id" = @next_owner_user_id,
                    "after_list_type" = @next_list_type,
                    "pass_changed" = "pass_changed" OR @page_changed,
                    "locked_until_ms" = clock.now_ms + @lease_ms,
                    "updated_at_ms" = clock.now_ms,
                    "last_error" = NULL
                FROM clock
                WHERE "id" = 1
                  AND "lease_owner" = @lease_owner
                  AND "claim_token" = @claim_token
                  AND "locked_until_ms" >= clock.now_ms;
                """,
            "relationship-projection-rebuild-commit-page",
            command =>
            {
                command.Parameters.AddWithValue(
                    "next_owner_user_id", NpgsqlDbType.Bigint, nextOwnerUserId);
                command.Parameters.AddWithValue(
                    "next_list_type", NpgsqlDbType.Smallint, (short)nextListType);
                command.Parameters.AddWithValue("page_changed", NpgsqlDbType.Boolean, pageChanged);
            },
            ct);
    }

    public async Task<RelationshipProjectionRebuildPassResult?> CompletePassAsync(
        RelationshipProjectionRebuildLease lease,
        bool finalPageChanged,
        TimeSpan stablePollInterval,
        CancellationToken ct = default)
    {
        ValidateLease(lease);
        if (stablePollInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(stablePollInterval));
        var pollMs = checked((long)stablePollInterval.TotalMilliseconds);
        var sql = schema.GetOrAddCommandText(
            "relationship-projection-rebuild-complete-pass",
            static current => $"""
                WITH clock AS (
                    SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
                )
                UPDATE {current.RelationshipProjectionRebuildStateTableSql} AS state
                SET "after_owner_user_id" = NULL,
                    "after_list_type" = NULL,
                    "pass_number" = "pass_number" + 1,
                    "stable_passes" = CASE
                        WHEN "pass_changed" OR @final_page_changed THEN 0
                        ELSE "stable_passes" + 1
                    END,
                    "pass_changed" = FALSE,
                    "lease_owner" = NULL,
                    "claim_token" = NULL,
                    "locked_until_ms" = NULL,
                    "next_attempt_at_ms" = clock.now_ms + CASE
                        WHEN NOT ("pass_changed" OR @final_page_changed)
                             AND "stable_passes" + 1 >= 2
                        THEN @stable_poll_ms
                        ELSE 0
                    END,
                    "last_error" = NULL,
                    "updated_at_ms" = clock.now_ms
                FROM clock
                WHERE "id" = 1
                  AND "lease_owner" = @lease_owner
                  AND "claim_token" = @claim_token
                  AND "locked_until_ms" >= clock.now_ms
                RETURNING "pass_number", "stable_passes", "next_attempt_at_ms";
                """);

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        AddLeaseParameters(command, lease);
        command.Parameters.AddWithValue(
            "final_page_changed", NpgsqlDbType.Boolean, finalPageChanged);
        command.Parameters.AddWithValue("stable_poll_ms", NpgsqlDbType.Bigint, pollMs);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;
        return new RelationshipProjectionRebuildPassResult(
            reader.GetInt64(0),
            reader.GetInt32(1),
            reader.GetInt64(2));
    }

    public async Task<bool> ReleaseFailureAsync(
        RelationshipProjectionRebuildLease lease,
        string errorCode,
        TimeSpan retryDelay,
        CancellationToken ct = default)
    {
        ValidateLease(lease);
        if (string.IsNullOrWhiteSpace(errorCode) || errorCode.Length > 128)
            throw new ArgumentException("Rebuild error code is invalid.", nameof(errorCode));
        if (retryDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(retryDelay));
        var retryMs = checked((long)retryDelay.TotalMilliseconds);
        var sql = schema.GetOrAddCommandText(
            "relationship-projection-rebuild-release-failure",
            static current => $"""
                WITH clock AS (
                    SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
                )
                UPDATE {current.RelationshipProjectionRebuildStateTableSql} AS state
                SET "lease_owner" = NULL,
                    "claim_token" = NULL,
                    "locked_until_ms" = NULL,
                    "next_attempt_at_ms" = clock.now_ms + @retry_ms,
                    "last_error" = @last_error,
                    "updated_at_ms" = clock.now_ms
                FROM clock
                WHERE "id" = 1
                  AND "lease_owner" = @lease_owner
                  AND "claim_token" = @claim_token;
                """);
        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        AddLeaseParameters(command, lease);
        command.Parameters.AddWithValue("retry_ms", NpgsqlDbType.Bigint, retryMs);
        command.Parameters.AddWithValue("last_error", NpgsqlDbType.Varchar, errorCode);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
    }

    private async Task<bool> UpdateLeaseAsync(
        RelationshipProjectionRebuildLease lease,
        TimeSpan leaseDuration,
        Func<RealtimeDatabaseSchema, string> sqlFactory,
        string cacheKey,
        Action<NpgsqlCommand>? addParameters,
        CancellationToken ct)
    {
        ValidateLease(lease);
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));
        var leaseMs = checked((long)leaseDuration.TotalMilliseconds);
        var sql = schema.GetOrAddCommandText(cacheKey, sqlFactory);
        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        AddLeaseParameters(command, lease);
        command.Parameters.AddWithValue("lease_ms", NpgsqlDbType.Bigint, leaseMs);
        addParameters?.Invoke(command);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
    }

    private static void AddLeaseParameters(
        NpgsqlCommand command,
        RelationshipProjectionRebuildLease lease)
    {
        command.Parameters.AddWithValue("lease_owner", NpgsqlDbType.Varchar, lease.LeaseOwner);
        command.Parameters.AddWithValue("claim_token", NpgsqlDbType.Varchar, lease.ClaimToken);
    }

    private static void ValidateOwnerAndDuration(string leaseOwner, TimeSpan leaseDuration)
    {
        if (string.IsNullOrWhiteSpace(leaseOwner) || leaseOwner.Length > 128)
            throw new ArgumentException("Lease owner is invalid.", nameof(leaseOwner));
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));
    }

    private static void ValidateLease(RelationshipProjectionRebuildLease lease)
    {
        ArgumentNullException.ThrowIfNull(lease);
        if (string.IsNullOrWhiteSpace(lease.LeaseOwner) || lease.LeaseOwner.Length > 128
            || lease.ClaimToken.Length != 32)
        {
            throw new ArgumentException("Relationship projection rebuild lease is invalid.", nameof(lease));
        }
    }
}
