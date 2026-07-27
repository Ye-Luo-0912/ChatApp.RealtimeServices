using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using ChatApp.RealtimeServices.Options;
using ChatApp.RealtimeServices.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace ChatApp.RealtimeServices.Diagnostics;

public static class RealtimeObservabilityRegistration
{
    public static ObservabilityOptions AddRealtimeObservability(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var options = configuration
            .GetSection(ObservabilityOptions.SectionName)
            .Get<ObservabilityOptions>()
            ?? new ObservabilityOptions();
        if (!options.IsValid())
            throw new InvalidOperationException("Observability 配置无效。");

        var realtime = configuration
            .GetSection("Realtime")
            .Get<RealtimeOptions>()
            ?? throw new InvalidOperationException("Realtime 配置节缺失。");
        var instanceId = realtime.InstanceId.Equals(
            "auto",
            StringComparison.OrdinalIgnoreCase)
            ? Environment.MachineName
            : realtime.InstanceId;

        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(options));
        services.AddHostedService<OutboxMetricsCollector>();
        services
            .AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(
                realtime.ServiceName,
                serviceInstanceId: instanceId))
            .WithMetrics(metrics =>
            {
                metrics.AddMeter(
                    RealtimeMetrics.MeterName,
                    RealtimeNatsTelemetry.MeterName,
                    RoutingMetrics.DefaultMeterName,
                    "System.Runtime",
                    "Microsoft.AspNetCore.Hosting",
                    "Microsoft.AspNetCore.Server.Kestrel",
                    "Npgsql");
                if (options.PrometheusEnabled)
                {
                    metrics.AddPrometheusExporter(prometheus =>
                        prometheus.ScrapeResponseCacheDurationMilliseconds =
                            options.PrometheusCacheMilliseconds);
                }

                if (options.OtlpEnabled)
                {
                    metrics.AddOtlpExporter(exporter =>
                        exporter.Endpoint = new Uri(options.OtlpEndpoint));
                }
            })
            .WithTracing(tracing =>
            {
                tracing
                    .AddSource(RealtimeTelemetry.ActivitySourceName)
                    .AddAspNetCoreInstrumentation(instrumentation =>
                        instrumentation.Filter = context =>
                            !IsManagementPath(context.Request.Path))
                    .SetSampler(new ParentBasedSampler(
                        new TraceIdRatioBasedSampler(options.TraceSampleRatio)));
                if (options.OtlpEnabled)
                {
                    tracing.AddOtlpExporter(exporter =>
                        exporter.Endpoint = new Uri(options.OtlpEndpoint));
                }
            });

        return options;
    }

    private static bool IsManagementPath(PathString path) =>
        path.StartsWithSegments("/live")
        || path.StartsWithSegments("/ready")
        || path.StartsWithSegments("/metrics")
        || path.StartsWithSegments("/diagnostics");
}
