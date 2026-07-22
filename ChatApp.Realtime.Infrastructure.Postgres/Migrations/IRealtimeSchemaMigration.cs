using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

public interface IRealtimeSchemaMigration
{
    int Version { get; }
    string Name { get; }

    Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken);
}
