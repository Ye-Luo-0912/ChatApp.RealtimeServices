using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Initialization;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Postgres.DependencyInjection;

public static class RealtimePostgresRegistration
{
    public static IServiceCollection AddRealtimeInfrastructurePostgres(
        this IServiceCollection services,
        string? connectionString,
        string schema,
        string messageStoreProvider)
    {
        services.AddSingleton(sp => new RealtimeDatabaseClient(
            connectionString,
            sp.GetRequiredService<ILogger<RealtimeDatabaseClient>>()));

        services.AddSingleton(new RealtimeDatabaseSchema(schema));

        if (ShouldUseEfCoreMessageStore(connectionString, messageStoreProvider))
        {
            RealtimeDbContext.ConfigureSchema(schema);
            services.AddPooledDbContextFactory<RealtimeDbContext>(options =>
            {
                options.UseNpgsql(connectionString);
            });

            services.RemoveAll<IRealtimeMessageStore>();
            services.AddSingleton<IRealtimeMessageStore, EfCoreRealtimeMessageStore>();
        }
        else if (ShouldUseNpgsqlMessageStore(connectionString, messageStoreProvider))
        {
            services.RemoveAll<IRealtimeMessageStore>();
            services.AddSingleton<IRealtimeMessageStore, NpgsqlRealtimeMessageStore>();
        }

        return services;
    }

    public static IServiceCollection AddRealtimeDatabaseInitializer(
        this IServiceCollection services,
        bool initializeSchemaOnStart,
        string? connectionString)
    {
        if (!initializeSchemaOnStart || string.IsNullOrWhiteSpace(connectionString))
        {
            return services;
        }

        services.AddHostedService<RealtimeDatabaseInitializer>();
        return services;
    }

    private static bool ShouldUseEfCoreMessageStore(
        string? connectionString,
        string messageStoreProvider)
    {
        return !string.IsNullOrWhiteSpace(connectionString)
               && messageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase);
    }

    private static bool ShouldUseNpgsqlMessageStore(
        string? connectionString,
        string messageStoreProvider)
    {
        return !string.IsNullOrWhiteSpace(connectionString)
               && messageStoreProvider.Equals("Npgsql", StringComparison.OrdinalIgnoreCase);
    }
}
