using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

public interface IRealtimeSchemaMigration
{
    int Version { get; }
    string Name { get; }

    /// <summary>
    /// 为 false 时迁移自行管理提交边界（如 CREATE INDEX CONCURRENTLY）。
    /// <para>
    /// 七-7：CONCURRENTLY 语句不能在事务内执行，此类迁移须设为 <c>false</c>，
    /// 并通过 <see cref="ConcurrentIndexHelper.EnsureValidAsync"/> 创建索引，
    /// 以处理构建被中断后遗留的 INVALID 索引。默认为 true（在事务内执行）。
    /// </para>
    /// </summary>
    bool RequiresTransaction => true;

    Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken);
}
