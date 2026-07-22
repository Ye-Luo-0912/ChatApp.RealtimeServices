using System.Diagnostics;
using ChatApp.Realtime.Abstractions.Diagnostics;
using NATS.Client.Core;

namespace ChatApp.Realtime.Infrastructure.Nats.Diagnostics;

internal static class NatsTraceContext
{
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
}
