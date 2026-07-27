using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// LongTerm-3：CONCURRENTLY 索引创建的统一恢复助手。
/// <para>
/// <c>CREATE INDEX CONCURRENTLY IF NOT EXISTS</c> 在构建被中断后会留下同名 INVALID 索引，
/// 再次执行时 <c>IF NOT EXISTS</c> 会误判为已完成，导致迁移被登记为成功但索引不可用。
/// </para>
/// <para>
/// 本助手按以下顺序处理：
/// 1. 查询 <c>pg_index.indisvalid</c>；已有效 → 跳过。
/// 2. INVALID → <c>DROP INDEX CONCURRENTLY</c> 后重建。
/// 3. 不存在 → <c>CREATE INDEX CONCURRENTLY</c>。
/// 4. 重建/创建后再次验证，仍 INVALID 则抛异常（由迁移 runner 暂停，不记为已应用）。
/// </para>
/// </summary>
internal static class ConcurrentIndexHelper
{
    /// <summary>
    /// 确保指定索引存在且有效；INVALID 时自动 DROP 后重建。
    /// </summary>
    /// <param name="connection">不能处于事务中（CONCURRENTLY 要求）。</param>
    /// <param name="quotedSchema">已加引号的 schema 名，例如 <c>"realtime"</c>。</param>
    /// <param name="schemaName">未加引号的 schema 名，用于查 <c>pg_namespace</c>。</param>
    /// <param name="indexName">索引名（不含 schema 前缀）。</param>
    /// <param name="createIndexSql">完整的 <c>CREATE INDEX CONCURRENTLY</c> 语句（不要带 <c>IF NOT EXISTS</c>）。</param>
    /// <param name="cancellationToken"></param>
    public static async Task EnsureValidAsync(
        NpgsqlConnection connection,
        string quotedSchema,
        string schemaName,
        string indexName,
        string createIndexSql,
        CancellationToken cancellationToken)
    {
        var state = await GetIndexStateAsync(connection, schemaName, indexName, cancellationToken)
            .ConfigureAwait(false);

        if (state == IndexState.Valid)
            return;

        if (state == IndexState.Invalid)
        {
            await using var drop = new NpgsqlCommand(
                $"DROP INDEX CONCURRENTLY {quotedSchema}.\"{indexName}\";",
                connection);
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var create = new NpgsqlCommand(createIndexSql, connection);
        await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        var afterState = await GetIndexStateAsync(connection, schemaName, indexName, cancellationToken)
            .ConfigureAwait(false);
        if (afterState != IndexState.Valid)
        {
            throw new InvalidOperationException(
                $"索引 {schemaName}.{indexName} 创建后仍为 {afterState}（可能构建被中断）。" +
                $"请手动执行 DROP INDEX CONCURRENTLY {quotedSchema}.\"{indexName}\"; 后重跑迁移。");
        }
    }

    private enum IndexState { Absent, Invalid, Valid }

    private static async Task<IndexState> GetIndexStateAsync(
        NpgsqlConnection connection,
        string schemaName,
        string indexName,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT i.indisvalid
            FROM pg_class c
            JOIN pg_index i ON i.indexrelid = c.oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = @schema
              AND c.relname = @index_name;
            """,
            connection);
        command.Parameters.AddWithValue("schema", schemaName);
        command.Parameters.AddWithValue("index_name", indexName);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (result is null)
            return IndexState.Absent;
        return (bool)result ? IndexState.Valid : IndexState.Invalid;
    }
}
