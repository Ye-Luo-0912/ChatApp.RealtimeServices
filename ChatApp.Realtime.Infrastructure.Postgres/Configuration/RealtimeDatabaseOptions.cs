namespace ChatApp.Realtime.Infrastructure.Postgres.Configuration;

public sealed class RealtimeDatabaseOptions
{
    public string Schema { get; init; } = "realtime";
    public string MessageStoreProvider { get; init; } = "Noop";
    public bool InitializeSchemaOnStart { get; init; }
}
