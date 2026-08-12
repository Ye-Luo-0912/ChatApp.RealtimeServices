using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;

namespace ChatApp.Realtime.Tests;

public sealed class PerformanceMigrationCatalogTests
{
    [Fact]
    public void DefaultCatalog_EndsWithRelationshipProjectionHistoryMigration()
    {
        var migrations = RealtimeSchemaMigrationRunner.DefaultMigrations();

        var migration = Assert.IsType<Migration063_RelationshipProjectionHistory>(migrations[^1]);
        Assert.Equal(63, migration.Version);
        Assert.True(((IRealtimeSchemaMigration)migration).RequiresTransaction);
        Assert.IsType<Migration062_RelationshipProjectionRebuilder>(migrations[^2]);
        Assert.Equal(
            migrations.Count,
            migrations.Select(item => item.Version).Distinct().Count());
    }
}

public sealed class RealtimeDatabaseSchemaCacheTests
{
    [Fact]
    public void CommandTextCache_ReusesOneImmutableValuePerSchemaAndKey()
    {
        var schema = new RealtimeDatabaseSchema("cache_one");
        var factoryCalls = 0;

        var values = Enumerable.Range(0, 64)
            .AsParallel()
            .Select(_ => schema.GetOrAddCommandText(
                "hot-path",
                current =>
                {
                    Interlocked.Increment(ref factoryCalls);
                    return $"SELECT * FROM {current.MessagesTableSql}";
                }))
            .ToArray();

        Assert.Equal(1, factoryCalls);
        Assert.All(values, value => Assert.Same(values[0], value));
        Assert.Contains("cache_one", values[0], StringComparison.Ordinal);
    }

    [Fact]
    public void CommandTextCache_DoesNotCrossSchemaInstances()
    {
        var firstSchema = new RealtimeDatabaseSchema("cache_first");
        var secondSchema = new RealtimeDatabaseSchema("cache_second");

        var first = firstSchema.GetOrAddCommandText(
            "hot-path",
            static schema => $"SELECT * FROM {schema.MessagesTableSql}");
        var second = secondSchema.GetOrAddCommandText(
            "hot-path",
            static schema => $"SELECT * FROM {schema.MessagesTableSql}");

        Assert.NotSame(first, second);
        Assert.Contains("cache_first", first, StringComparison.Ordinal);
        Assert.Contains("cache_second", second, StringComparison.Ordinal);
    }
}
