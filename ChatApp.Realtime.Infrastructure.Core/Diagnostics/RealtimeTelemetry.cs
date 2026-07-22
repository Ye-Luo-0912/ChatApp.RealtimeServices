using System.Diagnostics;

namespace ChatApp.Realtime.Infrastructure.Core.Diagnostics;

public static class RealtimeTelemetry
{
    public const string ActivitySourceName = "ChatApp.RealtimeServices";

    private static readonly ActivitySource Source = new(ActivitySourceName, "1.0.0");

    public static Activity? StartConsumer(
        string operation,
        ActivityContext parentContext)
    {
        var activity = parentContext.TraceId == default
            ? Source.StartActivity(operation, ActivityKind.Consumer)
            : Source.StartActivity(operation, ActivityKind.Consumer, parentContext);
        activity?.SetTag("messaging.system", "nats");
        activity?.SetTag("messaging.operation.name", "process");
        return activity;
    }

    public static Activity? StartOutboxPublish(ActivityContext parentContext)
    {
        var activity = parentContext.TraceId == default
            ? Source.StartActivity("outbox.event.publish", ActivityKind.Producer)
            : Source.StartActivity(
                "outbox.event.publish",
                ActivityKind.Producer,
                parentContext);
        activity?.SetTag("messaging.system", "nats");
        activity?.SetTag("messaging.operation.name", "publish");
        return activity;
    }

    public static void RecordException(Activity? activity, Exception exception)
    {
        activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity?.SetTag("error.type", exception.GetType().FullName);
    }
}
