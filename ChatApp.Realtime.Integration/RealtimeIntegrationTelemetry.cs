using System.Diagnostics;
using ChatApp.Realtime.Abstractions.Auth;
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

    /// <summary>
    /// 网关在发布入站/回执/历史查询时应注入已认证身份头，RealtimeServices 在 Trust 开启时优先信任这些头。
    /// </summary>
    public static NatsHeaders CreateIdentityHeaders(
        long userId,
        string? sessionId = null,
        NatsHeaders? existing = null)
    {
        var headers = existing ?? CreatePropagationHeaders() ?? new NatsHeaders();
        headers[RealtimeIdentityHeaders.UserId] = userId.ToString();
        if (!string.IsNullOrWhiteSpace(sessionId))
            headers[RealtimeIdentityHeaders.SessionId] = sessionId;
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
