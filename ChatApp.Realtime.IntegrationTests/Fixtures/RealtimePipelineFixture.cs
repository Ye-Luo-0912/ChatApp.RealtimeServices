using System.Net;
using System.Net.Sockets;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.RealtimeServices.DependencyInjection;
using ChatApp.RealtimeServices.Diagnostics;
using ChatApp.RealtimeServices.Options;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.Nats;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.IntegrationTests.Fixtures;

/// <summary>
/// Shared NATS(JetStream) + Postgres + in-process RealtimeServices host for Gateway→NATS→Realtime→PG e2e.
/// </summary>
public sealed class RealtimePipelineFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    private readonly NatsContainer _nats = new NatsBuilder()
        .WithImage("nats:2.10-alpine")
        .Build();

    private WebApplication? _host;
    private HttpClient? _http;

    public string NatsUrl { get; private set; } = "";
    public string PostgresConnectionString { get; private set; } = "";
    public string InstanceId { get; } = $"e2e-{Guid.NewGuid():N}"[..20];
    public string GatewayConsumerPrefix { get; } = "chatapp-e2e";

    public async Task InitializeAsync()
    {
        await Task.WhenAll(
            _postgres.StartAsync(),
            _nats.StartAsync()).ConfigureAwait(false);

        PostgresConnectionString = _postgres.GetConnectionString();
        NatsUrl = SanitizeNatsUrl(_nats.GetConnectionString());

        var httpPort = GetFreeTcpPort();
        var contentRoot = FindRealtimeServicesContentRoot();
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development,
            ContentRootPath = contentRoot,
            ApplicationName = "ChatApp.RealtimeServices"
        });

        builder.WebHost.UseUrls($"http://127.0.0.1:{httpPort}");
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["ConnectionStrings:RealtimeDatabase"] = PostgresConnectionString,
            ["ConnectionStrings:Garnet"] = "",
            ["Nats:Url"] = NatsUrl,
            ["Nats:Mode"] = "JetStream",
            ["Nats:Trust:RequireGatewayIdentity"] = "true",
            ["Nats:JetStream:Replicas"] = "1",
            ["Realtime:InstanceId"] = InstanceId,
            ["Realtime:ServiceName"] = "ChatApp.RealtimeServices.E2E",
            ["RealtimeDatabase:MessageStoreProvider"] = "Npgsql",
            ["RealtimeDatabase:InitializeSchemaOnStart"] = "true",
            ["RealtimeDatabase:Schema"] = "realtime",
            ["Observability:PrometheusEnabled"] = "false",
            ["Observability:OtlpEnabled"] = "false",
            ["Ops:ApiKey"] = ""
        });

        builder.Services.Configure<OpsOptions>(builder.Configuration.GetSection(OpsOptions.SectionName));
        builder.Services.AddRealtimeServices(builder.Configuration, builder.Environment);
        builder.Services.AddRealtimeObservability(builder.Configuration);

        _host = builder.Build();
        _host.MapGet("/live", () => Results.Ok(new
        {
            status = "alive",
            timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        }));
        _host.MapGet("/ready", async (RealtimeHealthService health, CancellationToken ct) =>
        {
            var snapshot = await health.CheckAsync(ct).ConfigureAwait(false);
            return snapshot.IsReady
                ? Results.Ok(snapshot)
                : Results.Json(snapshot, statusCode: StatusCodes.Status503ServiceUnavailable);
        });

        await _host.StartAsync().ConfigureAwait(false);
        _http = new HttpClient
        {
            BaseAddress = new Uri($"http://127.0.0.1:{httpPort}"),
            Timeout = TimeSpan.FromSeconds(5)
        };
        await WaitUntilReadyAsync().ConfigureAwait(false);
    }

    public NatsRealtimeMessageBus CreateBus(string? instanceSuffix = null)
    {
        return new NatsRealtimeMessageBus(
            new RealtimeIntegrationOptions
            {
                Url = NatsUrl,
                ClientName = "chatapp-realtime-e2e",
                InstanceId = string.IsNullOrWhiteSpace(instanceSuffix)
                    ? InstanceId
                    : $"{InstanceId}-{instanceSuffix}",
                GatewayConsumerPrefix = GatewayConsumerPrefix,
                ManageStreams = false,
                HistoryRequestTimeoutMs = 15_000,
                Replicas = 1
            },
            NullLogger<NatsRealtimeMessageBus>.Instance);
    }

    public async Task DisposeAsync()
    {
        _http?.Dispose();
        if (_host is not null)
        {
            await _host.StopAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            await _host.DisposeAsync().ConfigureAwait(false);
        }

        await Task.WhenAll(
            _postgres.DisposeAsync().AsTask(),
            _nats.DisposeAsync().AsTask()).ConfigureAwait(false);
    }

    private async Task WaitUntilReadyAsync()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        Exception? last = null;
        string? lastReadyResponse = null;
        while (!timeout.IsCancellationRequested)
        {
            try
            {
                using var live = await _http!.GetAsync("/live", timeout.Token).ConfigureAwait(false);
                live.EnsureSuccessStatusCode();
                using var ready = await _http.GetAsync("/ready", timeout.Token).ConfigureAwait(false);
                if (ready.IsSuccessStatusCode)
                    return;

                lastReadyResponse = await ready.Content
                    .ReadAsStringAsync(timeout.Token)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (timeout.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                last = ex;
            }

            try
            {
                await Task.Delay(500, timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (timeout.IsCancellationRequested)
            {
                break;
            }
        }

        throw new TimeoutException(
            "RealtimeServices host did not become ready in time. Last /ready response: " +
            (string.IsNullOrWhiteSpace(lastReadyResponse)
                ? "<none>"
                : lastReadyResponse[..Math.Min(lastReadyResponse.Length, 4096)]),
            last);
    }

    private static string SanitizeNatsUrl(string connectionString)
    {
        if (!Uri.TryCreate(connectionString, UriKind.Absolute, out var uri))
            return connectionString;

        if (string.IsNullOrEmpty(uri.UserInfo) || uri.UserInfo is ":")
            return $"nats://{uri.Host}:{uri.Port}";

        return connectionString;
    }

    private static int GetFreeTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static string FindRealtimeServicesContentRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "ChatApp.RealtimeServices", "appsettings.json");
            if (File.Exists(candidate))
                return Path.Combine(dir.FullName, "ChatApp.RealtimeServices");
            dir = dir.Parent;
        }

        throw new InvalidOperationException(
            "Unable to locate ChatApp.RealtimeServices/appsettings.json for test host content root.");
    }
}
