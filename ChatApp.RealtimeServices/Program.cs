using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.RealtimeServices.DependencyInjection;
using ChatApp.RealtimeServices.Diagnostics;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;

if (args.Length > 0 && args[0].Equals("--health-check", StringComparison.OrdinalIgnoreCase))
    return await RunHealthCheckCommandAsync(args).ConfigureAwait(false);

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
