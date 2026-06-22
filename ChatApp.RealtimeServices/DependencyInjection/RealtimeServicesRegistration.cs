using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Core.DependencyInjection;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.DependencyInjection;
using ChatApp.Realtime.Infrastructure.Postgres.Configuration;
using ChatApp.Realtime.Infrastructure.Postgres.DependencyInjection;
using ChatApp.Realtime.Infrastructure.Redis.DependencyInjection;
using ChatApp.RealtimeServices.Diagnostics;
using ChatApp.RealtimeServices.Options;
using ChatApp.RealtimeServices.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.DependencyInjection;

public static class RealtimeServicesRegistration
{
    public static IServiceCollection AddRealtimeServices(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var realtimeOptions = BindRealtimeOptions(configuration);
        var natsOptions = BindNatsOptions(configuration);
        var databaseOptions = BindDatabaseOptions(configuration);
        var connectionOptions = BindConnectionOptions(configuration);
        var warnings = BuildWarnings(configuration, natsOptions, databaseOptions, connectionOptions);

        services.AddSingleton<IOptions<RealtimeOptions>>(Microsoft.Extensions.Options.Options.Create(realtimeOptions));
        services.AddSingleton<IOptions<NatsOptions>>(Microsoft.Extensions.Options.Options.Create(natsOptions));
        services.AddSingleton<IOptions<RealtimeDatabaseOptions>>(Microsoft.Extensions.Options.Options.Create(databaseOptions));
        services.AddSingleton<IOptions<RealtimeConnectionOptions>>(Microsoft.Extensions.Options.Options.Create(connectionOptions));
        services.AddSingleton(new RealtimeConfigurationWarnings(warnings));

        services.AddRealtimeInfrastructureCore();
        services.AddRealtimeInfrastructureRedis(connectionOptions.Garnet);
        services.AddRealtimeInfrastructurePostgres(
            connectionOptions.RealtimeDatabase,
            databaseOptions.Schema,
            databaseOptions.MessageStoreProvider);
        services.AddRealtimeInfrastructureNats(
            CreateRealtimeQueueOptions(natsOptions));

        services.AddHostedService<RealtimeStartupReporter>();

        if (databaseOptions.InitializeSchemaOnStart
            && !string.IsNullOrWhiteSpace(connectionOptions.RealtimeDatabase))
        {
            services.AddHostedService<RealtimeDatabaseInitializer>();
        }

        services.AddHostedService<RealtimeEventWorker>();
        services.AddHostedService<IncomingMessageWorker>();

        return services;
    }

    private static RealtimeOptions BindRealtimeOptions(IConfiguration configuration)
    {
        var section = configuration.GetSection("Realtime");
        var options = section.Get<RealtimeOptions>()
            ?? throw new InvalidOperationException("Realtime 配置节缺失。");

        if (string.IsNullOrWhiteSpace(options.ServiceName))
            throw new InvalidOperationException("Realtime:ServiceName 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.InstanceId))
            throw new InvalidOperationException("Realtime:InstanceId 为必填配置。");

        return options;
    }

    private static NatsOptions BindNatsOptions(IConfiguration configuration)
    {
        var section = configuration.GetSection("Nats");
        var options = section.Get<NatsOptions>()
            ?? throw new InvalidOperationException("Nats 配置节缺失。");

        if (string.IsNullOrWhiteSpace(options.QueueGroup))
            throw new InvalidOperationException("Nats:QueueGroup 为必填配置。");
        if (options.Subjects is null)
            throw new InvalidOperationException("Nats:Subjects 配置节缺失。");
        if (string.IsNullOrWhiteSpace(options.Subjects.IncomingMessages))
            throw new InvalidOperationException("Nats:Subjects:IncomingMessages 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.RealtimeEvents))
            throw new InvalidOperationException("Nats:Subjects:RealtimeEvents 为必填配置。");

        return options;
    }

    private static RealtimeDatabaseOptions BindDatabaseOptions(IConfiguration configuration)
    {
        var section = configuration.GetSection("RealtimeDatabase");
        var raw = section.Get<RealtimeDatabaseOptions>();

        return new RealtimeDatabaseOptions
        {
            Schema = Normalize(raw?.Schema) ?? "realtime",
            MessageStoreProvider = Normalize(raw?.MessageStoreProvider) ?? "Noop",
            InitializeSchemaOnStart = raw?.InitializeSchemaOnStart ?? false
        };
    }

    private static RealtimeConnectionOptions BindConnectionOptions(IConfiguration configuration)
    {
        var section = configuration.GetSection("ConnectionStrings");
        var raw = section.Get<RealtimeConnectionOptions>()
            ?? throw new InvalidOperationException("ConnectionStrings 配置节缺失。");

        if (string.IsNullOrWhiteSpace(raw.Garnet))
            throw new InvalidOperationException("ConnectionStrings:Garnet 为必填配置。");

        return new RealtimeConnectionOptions
        {
            Garnet = raw.Garnet,
            RealtimeDatabase = Normalize(configuration["ConnectionStrings:RealtimeDatabase"])
                ?? Normalize(configuration["ConnectionStrings:DefaultConnection"])
        };
    }

    private static RealtimeQueueOptions CreateRealtimeQueueOptions(NatsOptions options)
    {
        return new RealtimeQueueOptions
        {
            Provider = "Nats",
            Endpoint = options.Url,
            ConsumerGroup = options.QueueGroup,
            Topics = new RealtimeQueueTopics
            {
                IncomingMessages = options.Subjects.IncomingMessages,
                RealtimeEvents = options.Subjects.RealtimeEvents,
                MessagePersistence = options.Subjects.MessagePersistence
            }
        };
    }

    private static IReadOnlyList<string> BuildWarnings(
        IConfiguration configuration,
        NatsOptions natsOptions,
        RealtimeDatabaseOptions databaseOptions,
        RealtimeConnectionOptions connectionOptions)
    {
        var warnings = new List<string>();

        if (string.IsNullOrWhiteSpace(natsOptions.Url))
        {
            warnings.Add("Nats:Url 未配置，实时队列会回退为空实现。");
        }

        if (string.IsNullOrWhiteSpace(connectionOptions.RealtimeDatabase))
        {
            warnings.Add("ConnectionStrings:RealtimeDatabase 未配置，实时数据库客户端不会建立连接。");
        }
        else if (string.IsNullOrWhiteSpace(configuration["ConnectionStrings:RealtimeDatabase"])
                 && !string.IsNullOrWhiteSpace(configuration["ConnectionStrings:DefaultConnection"]))
        {
            warnings.Add("ConnectionStrings:RealtimeDatabase 未配置，已回退使用 ConnectionStrings:DefaultConnection。");
        }

        if (string.IsNullOrWhiteSpace(configuration["RealtimeDatabase:Schema"]))
        {
            warnings.Add($"RealtimeDatabase:Schema 未配置，已使用默认值 '{databaseOptions.Schema}'。");
        }

        if (databaseOptions.MessageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase)
            && string.IsNullOrWhiteSpace(connectionOptions.RealtimeDatabase))
        {
            warnings.Add("实时消息存储已指定为 EF Core，但 ConnectionStrings:RealtimeDatabase 未配置，运行时会回退到 P0 默认空实现。");
        }

        if (databaseOptions.MessageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase))
        {
            warnings.Add("当前启用了 EF Core 消息存储。Native AOT 发布下需要 compiled model，否则运行时可能失败。容器环境建议使用 Npgsql。");
        }

        if (databaseOptions.InitializeSchemaOnStart
            && string.IsNullOrWhiteSpace(connectionOptions.RealtimeDatabase))
        {
            warnings.Add("已要求启动时初始化实时数据库表结构，但实时数据库连接字符串未配置，初始化不会执行。");
        }

        return warnings;
    }

    private static string? Normalize(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
