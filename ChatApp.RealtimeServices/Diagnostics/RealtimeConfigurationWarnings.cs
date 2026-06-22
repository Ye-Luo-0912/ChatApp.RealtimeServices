namespace ChatApp.RealtimeServices.Diagnostics;

public sealed class RealtimeConfigurationWarnings
{
    public RealtimeConfigurationWarnings(IReadOnlyList<string> warnings)
    {
        Warnings = warnings;
    }

    public IReadOnlyList<string> Warnings { get; }
}
