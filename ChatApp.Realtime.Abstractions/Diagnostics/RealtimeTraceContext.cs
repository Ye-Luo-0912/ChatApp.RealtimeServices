using System.Diagnostics;

namespace ChatApp.Realtime.Abstractions.Diagnostics;

public static class RealtimeTraceContext
{
    public const string TraceParentHeader = "traceparent";
    public const string TraceStateHeader = "tracestate";

    public static string? CaptureTraceParent()
    {
        var activity = Activity.Current;
        return activity is { IdFormat: ActivityIdFormat.W3C }
            ? activity.Id
            : null;
    }

    public static string? CaptureTraceState() => Activity.Current?.TraceStateString;

    public static ActivityContext Parse(
        string? traceParent,
        string? traceState)
    {
        return ActivityContext.TryParse(
            traceParent,
            traceState,
            isRemote: true,
            out var context)
            ? context
            : default;
    }
}
