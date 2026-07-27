using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Configuration;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.RealtimeServices.DependencyInjection;
using ChatApp.RealtimeServices.Diagnostics;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;

if (args.Length > 0 && args[0].Equals("--health-check", StringComparison.OrdinalIgnoreCase))
    return await RunHealthCheckCommandAsync(args).ConfigureAwait(false);

if (args.Length > 0 && args[0].Equals("--migrate", StringComparison.OrdinalIgnoreCase))
    return await RunMigrateCommandAsync().ConfigureAwait(false);

try
{
    var builder = WebApplication.CreateSlimBuilder(args);
    builder.Configuration.AddJsonFile(
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".chatapp", "realtime.user.json"),
        optional: true,
        reloadOnChange: false);

    builder.Logging.ClearProviders();
    builder.Logging.AddSimpleConsole(options =>
    {
        options.SingleLine = true;
        options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
    });

    builder.Services.Configure<OpsOptions>(builder.Configuration.GetSection(OpsOptions.SectionName));
    builder.Services.AddRealtimeServices(builder.Configuration, builder.Environment);
    var observabilityOptions = builder.Services.AddRealtimeObservability(
        builder.Configuration);
    var app = builder.Build();
    if (observabilityOptions.PrometheusEnabled)
        app.UseOpenTelemetryPrometheusScrapingEndpoint();

    app.MapGet("/live", () => Results.Ok(new
    {
        status = "alive",
        timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
    }));
    app.MapGet("/ready", async (RealtimeHealthService health, CancellationToken ct) =>
    {
        var snapshot = await health.CheckAsync(ct).ConfigureAwait(false);
        return snapshot.IsReady ? Results.Ok(snapshot) : Results.Json(snapshot, statusCode: 503);
    });
    app.MapGet("/diagnostics/runtime", async (
        RealtimeMetrics metrics,
        IRealtimeOutboxStore outboxStore,
        CancellationToken ct) => Results.Ok(new
        {
            Runtime = metrics.GetSnapshot(),
            Outbox = await outboxStore.GetStatsAsync(ct).ConfigureAwait(false)
        }));

    var ops = app.MapGroup("/ops/outbox");
    ops.AddEndpointFilter(new OpsApiKeyEndpointFilter());
    ops.MapGet("/summary", async (IRealtimeOutboxStore outboxStore, CancellationToken ct) =>
    {
        var stats = await outboxStore.GetStatsAsync(ct).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return Results.Ok(new
        {
            stats.PendingCount,
            stats.DeadCount,
            stats.MaxAttemptCount,
            stats.OldestPendingAtMs,
            OldestPendingAgeMs = stats.OldestPendingAtMs is { } oldest
                ? Math.Max(0, now - oldest)
                : (long?)null,
            stats.OldestInFlightAtMs,
            GeneratedAtMs = now
        });
    });
    ops.MapGet("/", async (
        string? status,
        long? targetUserId,
        int? offset,
        int? limit,
        IRealtimeOutboxStore outboxStore,
        CancellationToken ct) =>
    {
        RealtimeOutboxStatus? parsed = null;
        if (!string.IsNullOrWhiteSpace(status))
        {
            if (Enum.TryParse<RealtimeOutboxStatus>(status, ignoreCase: true, out var named))
                parsed = named;
            else if (short.TryParse(status, out var numeric)
                     && Enum.IsDefined(typeof(RealtimeOutboxStatus), numeric))
                parsed = (RealtimeOutboxStatus)numeric;
            else
                return Results.BadRequest(new { error = "invalid_status" });
        }

        var items = await outboxStore.ListAsync(
                parsed,
                targetUserId,
                offset ?? 0,
                limit ?? 50,
                ct)
            .ConfigureAwait(false);
        return Results.Ok(new { items, offset = offset ?? 0, limit = limit ?? 50, returned = items.Count });
    });
    ops.MapGet("/{eventId}", async (
        string eventId,
        IRealtimeOutboxStore outboxStore,
        CancellationToken ct) =>
    {
        var item = await outboxStore.TryGetAsync(eventId, ct).ConfigureAwait(false);
        return item is null ? Results.NotFound(new { error = "not_found" }) : Results.Ok(item);
    });
    ops.MapPost("/{eventId}/replay", async (
        string eventId,
        IRealtimeOutboxStore outboxStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        CancellationToken ct) =>
    {
        if (string.IsNullOrWhiteSpace(eventId) || eventId.Length > 64)
            return Results.BadRequest(new { error = "invalid_event_id" });

        var existing = await outboxStore.TryGetAsync(eventId, ct).ConfigureAwait(false);
        if (existing is null)
            return Results.NotFound(new { error = "not_found", eventId });
        if (existing.Status == RealtimeOutboxStatus.Published)
            return Results.Conflict(new { error = "already_published", eventId });
        if (existing.Status != RealtimeOutboxStatus.Dead)
            return Results.Conflict(new { error = "not_dead", eventId, status = existing.Status.ToString() });

        var replayed = await outboxStore.ReplayDeadAsync(eventId, ct).ConfigureAwait(false);
        if (!replayed)
            return Results.NotFound(new { error = "not_found_or_not_dead", eventId });

        metrics.RecordOutboxReplay(1);
        outboxSignal.Notify();
        return Results.Ok(new { eventId, status = "pending" });
    });
    ops.MapPost("/replay", async (
        ReplayDeadBatchRequest? body,
        IRealtimeOutboxStore outboxStore,
        IRealtimeOutboxSignal outboxSignal,
        RealtimeMetrics metrics,
        CancellationToken ct) =>
    {
        var eventIds = body?.EventIds;
        if (eventIds is null || eventIds.Count == 0)
            return Results.BadRequest(new { error = "event_ids_required" });
        if (eventIds.Count > 500)
            return Results.BadRequest(new { error = "too_many_event_ids", max = 500 });

        var replayed = await outboxStore
            .ReplayDeadBatchAsync(eventIds, ct)
            .ConfigureAwait(false);
        if (replayed.Count > 0)
        {
            metrics.RecordOutboxReplay(replayed.Count);
            outboxSignal.Notify();
        }

        return Results.Ok(new
        {
            requested = eventIds.Count,
            replayed = replayed.Count,
            eventIds = replayed
        });
    });

    var opsMigrations = app.MapGroup("/ops/migrations");
    opsMigrations.AddEndpointFilter(new OpsApiKeyEndpointFilter());
    opsMigrations.MapGet("/progress", async (IRealtimeOpsQueryStore opsQuery, CancellationToken ct) =>
        Results.Ok(await opsQuery.GetMigrationProgressAsync(ct).ConfigureAwait(false)));

    var opsBacklogs = app.MapGroup("/ops/backlogs");
    opsBacklogs.AddEndpointFilter(new OpsApiKeyEndpointFilter());
    opsBacklogs.MapGet("/", async (IRealtimeOpsQueryStore opsQuery, CancellationToken ct) =>
        Results.Ok(await opsQuery.GetBacklogsAsync(ct).ConfigureAwait(false)));

    var realtimeOptions = app.Services.GetRequiredService<IOptions<RealtimeOptions>>().Value;
    app.Logger.LogInformation(
        "正在启动实时服务。服务名={ServiceName}；实例={InstanceId}；环境={Environment}",
        realtimeOptions.ServiceName,
        realtimeOptions.InstanceId,
        app.Environment.EnvironmentName);

    await app.RunAsync().ConfigureAwait(false);
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"ChatApp.RealtimeServices 启动失败：{ex.Message}");
    Console.Error.WriteLine(ex);
    return 1;
}

static async Task<int> RunHealthCheckCommandAsync(string[] commandArgs)
{
    var url = commandArgs.Length > 1 ? commandArgs[1] : "http://127.0.0.1:8080/ready";
    using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
    try
    {
        using var response = await client.GetAsync(url).ConfigureAwait(false);
        return response.IsSuccessStatusCode ? 0 : 1;
    }
    catch
    {
        return 1;
    }
}

static async Task<int> RunMigrateCommandAsync()
{
    // P0-1：C# migrations 是唯一事实来源。独立迁移命令供 Compose migration Job
    // 或 CI 调用，不在运行时迁移路径之外维护第二套手写 SQL。
    var configuration = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: false)
        .AddJsonFile(
            Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                ".chatapp",
                "realtime.user.json"),
            optional: true,
            reloadOnChange: false)
        .AddEnvironmentVariables()
        .Build();

    var connectionString = configuration.GetConnectionString("RealtimeDatabase");
    if (string.IsNullOrWhiteSpace(connectionString))
    {
        Console.Error.WriteLine("ConnectionStrings__RealtimeDatabase 未配置，无法执行迁移。");
        return 1;
    }

    var schema = configuration.GetSection("RealtimeDatabase:Schema").Get<string>() ?? "realtime";

    using var loggerFactory = LoggerFactory.Create(builder =>
    {
        builder.AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
        });
        builder.SetMinimumLevel(LogLevel.Information);
    });
    var migrateLogger = loggerFactory.CreateLogger("RealtimeMigrations");
    var clientLogger = loggerFactory.CreateLogger<RealtimeDatabaseClient>();

    await using var databaseClient = new RealtimeDatabaseClient(connectionString, clientLogger);
    var databaseSchema = new RealtimeDatabaseSchema(schema);
    var runner = new RealtimeSchemaMigrationRunner(databaseSchema, migrateLogger);

    migrateLogger.LogInformation("正在通过版本化迁移初始化实时数据库。数据库架构={Schema}", schema);

    const int maxAttempts = 30;
    for (var attempt = 1; ; attempt++)
    {
        try
        {
            await using var connection = await databaseClient
                .GetDataSource()
                .OpenConnectionAsync()
                .ConfigureAwait(false);
            await runner.MigrateAsync(connection).ConfigureAwait(false);
            migrateLogger.LogInformation("实时数据库版本化迁移完成。数据库架构={Schema}", schema);
            return 0;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (attempt >= maxAttempts)
            {
                migrateLogger.LogError(
                    ex,
                    "实时数据库迁移失败，已达到最大重试次数。尝试次数={Attempt}",
                    attempt);
                return 1;
            }

            migrateLogger.LogWarning(
                ex,
                "实时数据库迁移失败，将在短暂等待后重试。尝试次数={Attempt}/{MaxAttempts}",
                attempt,
                maxAttempts);
            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
        }
    }
}

file sealed class OpsApiKeyEndpointFilter : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(
        EndpointFilterInvocationContext context,
        EndpointFilterDelegate next)
    {
        var http = context.HttpContext;
        var env = http.RequestServices.GetRequiredService<IHostEnvironment>();
        var ops = http.RequestServices.GetRequiredService<IOptions<OpsOptions>>().Value;
        var configured = ops.ApiKey?.Trim();

        if (string.IsNullOrEmpty(configured))
        {
            if (env.IsProduction())
                return Results.Json(new { error = "ops_api_key_required" }, statusCode: 503);
            return await next(context).ConfigureAwait(false);
        }

        if (!http.Request.Headers.TryGetValue("X-Ops-Api-Key", out var provided)
            || !string.Equals(provided.ToString(), configured, StringComparison.Ordinal))
        {
            return Results.Json(new { error = "unauthorized" }, statusCode: 401);
        }

        return await next(context).ConfigureAwait(false);
    }
}

file sealed class ReplayDeadBatchRequest
{
    public List<string>? EventIds { get; init; }
}
