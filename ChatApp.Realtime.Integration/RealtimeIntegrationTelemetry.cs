using System.Diagnostics;
using ChatApp.Realtime.Abstractions.Diagnostics;
using NATS.Client.Core;

namespace ChatApp.Realtime.Integration;

public static class RealtimeIntegrationTelemetry
{
    public const string ActivitySourceName = "ChatApp.Realtime.Integration";

    private static readonly ActivitySource Source = new(ActivitySourceName, "1.0.0");

    public static Activity? StartProducer(string operation, string destination)
    {
        var activity = Source.StartActivity(operation, ActivityKind.Producer);
        activity?.SetTag("messaging.system", "nats");
        activity?.SetTag("messaging.destination.name", destination);
        activity?.SetTag("messaging.operation.name", "publish");
        return activity;
    }

    public static Activity? StartClient(string operation, string destination)
    {
        var activity = Source.StartActivity(operation, ActivityKind.Client);
        activity?.SetTag("messaging.system", "nats");
        activity?.SetTag("messaging.destination.name", destination);
        activity?.SetTag("messaging.operation.name", "request");
        return activity;
    }

    public static NatsHeaders? CreatePropagationHeaders()
    {
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        if (traceParent is null)
            return null;

        var headers = new NatsHeaders
        {
            [RealtimeTraceContext.TraceParentHeader] = traceParent
        };
        var traceState = RealtimeTraceContext.CaptureTraceState();
        if (!string.IsNullOrWhiteSpace(traceState))
            headers[RealtimeTraceContext.TraceStateHeader] = traceState;
        return headers;
    }

    public static ActivityContext ExtractParentContext(NatsHeaders? headers)
    {
        if (headers is null
            || !headers.TryGetValue(
                RealtimeTraceContext.TraceParentHeader,
                out var traceParent))
        {
            return default;
        }

        headers.TryGetValue(
            RealtimeTraceContext.TraceStateHeader,
            out var traceState);
        return RealtimeTraceContext.Parse(
            traceParent.ToString(),
            traceState.ToString());
    }

    public static void RecordException(Activity? activity, Exception exception)
    {
        activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity?.SetTag("error.type", exception.GetType().FullName);
    }
}
