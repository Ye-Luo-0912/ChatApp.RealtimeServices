using System.Net;
using System.Net.Sockets;
using ChatApp.Realtime.Integration;
using ChatApp.Realtime.Integration.Configuration;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.RealtimeServices.DependencyInjection;
using ChatApp.RealtimeServices.Diagnostics;
using ChatApp.RealtimeServices.Options;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
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
        await InitializeIdentitySchemaAsync().ConfigureAwait(false);

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
        // The pipeline suite validates transport, persistence and wire budgets;
        // its 40-message budget case must not be capped by the production
        // per-user 30-message sliding window.
        builder.Services.RemoveAll<IMessageRateLimiter>();
        builder.Services.AddSingleton<IMessageRateLimiter>(NoopMessageRateLimiter.Instance);

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

    /// <summary>
    /// Seeds the minimal shared Identity boundary used by the production
    /// authorization stores. Pipeline tests opt in explicitly so missing users
    /// continue to exercise the fail-closed path.
    /// </summary>
    public async Task EnsureUsersExistAsync(params long[] userIds)
    {
        var ids = userIds.Where(static id => id > 0).Distinct().ToArray();
        if (ids.Length == 0)
            return;

        await using var connection = new NpgsqlConnection(PostgresConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO public."AspNetUsers" ("Id")
            SELECT unnest(@user_ids)
            ON CONFLICT ("Id") DO NOTHING;
            """,
            connection);
        command.Parameters.AddWithValue("user_ids", ids);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public async Task EnsureDirectMessageAllowedAsync(long senderUserId, long receiverUserId)
    {
        await EnsureUsersExistAsync(senderUserId, receiverUserId).ConfigureAwait(false);

        await using var connection = new NpgsqlConnection(PostgresConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO public."T_UserFriendEntry" ("UserId", "FriendId", "IsDeleted")
            VALUES
                (@sender_id, @receiver_id, FALSE),
                (@receiver_id, @sender_id, FALSE)
            ON CONFLICT ("UserId", "FriendId")
            DO UPDATE SET "IsDeleted" = FALSE;
            """,
            connection);
        command.Parameters.AddWithValue("sender_id", senderUserId);
        command.Parameters.AddWithValue("receiver_id", receiverUserId);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
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

    private async Task InitializeIdentitySchemaAsync()
    {
        await using var connection = new NpgsqlConnection(PostgresConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            """
            CREATE TABLE public."AspNetUsers"
            (
                "Id" bigint PRIMARY KEY,
                "FriendRequestPolicy" smallint NOT NULL DEFAULT 1
            );

            CREATE TABLE public."T_BlockRecords"
            (
                "BlockerId" bigint NOT NULL,
                "BlockedUserId" bigint NOT NULL,
                PRIMARY KEY ("BlockerId", "BlockedUserId")
            );

            CREATE TABLE public."T_UserFriendEntry"
            (
                "UserId" bigint NOT NULL,
                "FriendId" bigint NOT NULL,
                "IsDeleted" boolean NOT NULL DEFAULT FALSE,
                PRIMARY KEY ("UserId", "FriendId")
            );
            """,
            connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
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
