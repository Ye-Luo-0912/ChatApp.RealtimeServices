using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.DependencyInjection;
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
        var realtimeOptions = ReadRealtimeOptions(configuration);
        var natsOptions = ReadNatsOptions(configuration);
        var databaseOptions = ReadRealtimeDatabaseOptions(configuration);
        var connectionOptions = ReadConnectionOptions(configuration);
        var warnings = ReadConfigurationWarnings(configuration, natsOptions, databaseOptions, connectionOptions);

        services.AddSingleton<IOptions<RealtimeOptions>>(Microsoft.Extensions.Options.Options.Create(realtimeOptions));
        services.AddSingleton<IOptions<NatsOptions>>(Microsoft.Extensions.Options.Options.Create(natsOptions));
        services.AddSingleton<IOptions<RealtimeDatabaseOptions>>(Microsoft.Extensions.Options.Options.Create(databaseOptions));
        services.AddSingleton<IOptions<RealtimeConnectionOptions>>(Microsoft.Extensions.Options.Options.Create(connectionOptions));
        services.AddSingleton(new RealtimeConfigurationWarnings(warnings));

        services.AddRealtimeInfrastructure(
            connectionOptions.Garnet,
            connectionOptions.RealtimeDatabase,
            databaseOptions.Schema,
            databaseOptions.MessageStoreProvider,
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

    private static RealtimeOptions ReadRealtimeOptions(IConfiguration configuration)
    {
        return new RealtimeOptions
        {
            ServiceName = GetRequiredValue(configuration, "Realtime:ServiceName"),
            InstanceId = GetRequiredValue(configuration, "Realtime:InstanceId"),
            WorkerIntervalMs = GetPositiveInt(configuration, "Realtime:WorkerIntervalMs", 1000),
            EnableDetailedErrors = GetBool(configuration, "Realtime:EnableDetailedErrors", false)
        };
    }

    private static NatsOptions ReadNatsOptions(IConfiguration configuration)
    {
        return new NatsOptions
        {
            Url = Normalize(configuration["Nats:Url"]),
            QueueGroup = GetRequiredValue(configuration, "Nats:QueueGroup"),
            Subjects = new NatsSubjectOptions
            {
                IncomingMessages = GetRequiredValue(configuration, "Nats:Subjects:IncomingMessages"),
                RealtimeEvents = GetRequiredValue(configuration, "Nats:Subjects:RealtimeEvents"),
                MessagePersistence = Normalize(configuration["Nats:Subjects:MessagePersistence"])
            }
        };
    }

    private static RealtimeDatabaseOptions ReadRealtimeDatabaseOptions(IConfiguration configuration)
    {
        return new RealtimeDatabaseOptions
        {
            Schema = Normalize(configuration["RealtimeDatabase:Schema"]) ?? "realtime",
            MessageStoreProvider = Normalize(configuration["RealtimeDatabase:MessageStoreProvider"]) ?? "Noop",
            InitializeSchemaOnStart = GetBool(configuration, "RealtimeDatabase:InitializeSchemaOnStart", false)
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

    private static RealtimeConnectionOptions ReadConnectionOptions(IConfiguration configuration)
    {
        return new RealtimeConnectionOptions
        {
            Garnet = GetRequiredValue(configuration, "ConnectionStrings:Garnet"),
            RealtimeDatabase =
                Normalize(configuration["ConnectionStrings:RealtimeDatabase"])
                ?? Normalize(configuration["ConnectionStrings:DefaultConnection"])
        };
    }

    private static IReadOnlyList<string> ReadConfigurationWarnings(
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

    private static string GetRequiredValue(IConfiguration configuration, string key)
    {
        var value = Normalize(configuration[key]);

        if (value is null)
        {
            throw new InvalidOperationException($"{key} 为必填配置。");
        }

        return value;
    }

    private static int GetPositiveInt(IConfiguration configuration, string key, int defaultValue)
    {
        var value = Normalize(configuration[key]);

        if (value is null)
        {
            return defaultValue;
        }

        if (!int.TryParse(value, out var parsed) || parsed <= 0)
        {
            throw new InvalidOperationException($"{key} 必须大于 0。");
        }

        return parsed;
    }

    private static bool GetBool(IConfiguration configuration, string key, bool defaultValue)
    {
        var value = Normalize(configuration[key]);

        if (value is null)
        {
            return defaultValue;
        }

        if (!bool.TryParse(value, out var parsed))
        {
            throw new InvalidOperationException($"{key} 必须是 true 或 false。");
        }

        return parsed;
    }

    private static string? Normalize(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
