using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

public interface IRealtimeSchemaMigration
{
    int Version { get; }
    string Name { get; }

    /// <summary>为 false 时迁移自行管理提交边界（如 CREATE INDEX CONCURRENTLY）。</summary>
    bool RequiresTransaction => true;

    Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken);
}
