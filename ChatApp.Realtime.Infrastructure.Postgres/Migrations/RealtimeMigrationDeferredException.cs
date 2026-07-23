namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 在线迁移主动暂停（如 MaxBatches），尚未完成，不应写入 schema_migrations。
/// </summary>
public sealed class RealtimeMigrationDeferredException : Exception
{
    public RealtimeMigrationDeferredException(int version, string name, string message)
        : base(message)
    {
        Version = version;
        Name = name;
    }

    public int Version { get; }
    public string Name { get; }
}
