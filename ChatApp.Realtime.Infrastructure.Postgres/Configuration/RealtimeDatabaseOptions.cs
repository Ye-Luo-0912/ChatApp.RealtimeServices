namespace ChatApp.Realtime.Infrastructure.Postgres.Configuration;

/// <summary>
/// Realtime 数据库选项。
/// <see cref="MessageStoreProvider"/>：生产必须为 <c>Npgsql</c>。
/// <c>EfCore</c> 仅 Development/Testing（不绑定附件，回执/删除语义不完整）。
/// </summary>
public sealed class RealtimeDatabaseOptions
{
    public string Schema { get; init; } = "realtime";

    /// <summary>Noop | Npgsql | EfCore（EfCore 仅 Dev/Testing）。</summary>
    public string MessageStoreProvider { get; init; } = "Noop";

    public bool InitializeSchemaOnStart { get; init; }
}
