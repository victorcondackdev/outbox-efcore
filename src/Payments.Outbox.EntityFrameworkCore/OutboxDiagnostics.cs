using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Names of the <see cref="ActivitySource"/> and <see cref="Meter"/> used by the outbox. Register them
/// with OpenTelemetry (<c>AddSource</c> / <c>AddMeter</c>) to export traces and metrics.
/// </summary>
public static class OutboxDiagnostics
{
    /// <summary>Name of the activity source.</summary>
    public const string ActivitySourceName = "Payments.Outbox";

    /// <summary>Name of the meter.</summary>
    public const string MeterName = "Payments.Outbox";

    /// <summary>Counter of successfully published messages.</summary>
    public const string PublishedCounterName = "outbox.messages.published";

    /// <summary>Counter of failed publish attempts.</summary>
    public const string FailedCounterName = "outbox.messages.failed";

    /// <summary>Counter of dead-lettered messages.</summary>
    public const string DeadLetteredCounterName = "outbox.messages.dead_lettered";

    /// <summary>Histogram of the delay between writing and publishing a message, in seconds.</summary>
    public const string LagHistogramName = "outbox.message.lag";
}

internal static class OutboxTelemetry
{
    private static readonly string? AssemblyVersion = typeof(OutboxTelemetry).Assembly.GetName().Version?.ToString();

    internal static readonly ActivitySource ActivitySource = new(OutboxDiagnostics.ActivitySourceName, AssemblyVersion);

    internal static readonly Meter Meter = new(OutboxDiagnostics.MeterName, AssemblyVersion);

    internal static readonly Counter<long> Published = Meter.CreateCounter<long>(
        OutboxDiagnostics.PublishedCounterName, "{message}", "Messages published by the outbox relay.");

    internal static readonly Counter<long> Failed = Meter.CreateCounter<long>(
        OutboxDiagnostics.FailedCounterName, "{message}", "Failed publish attempts.");

    internal static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>(
        OutboxDiagnostics.DeadLetteredCounterName, "{message}", "Messages dead-lettered after exhausting their attempts.");

    internal static readonly Histogram<double> Lag = Meter.CreateHistogram<double>(
        OutboxDiagnostics.LagHistogramName, "s", "Time between writing a message and publishing it.");

    internal static KeyValuePair<string, object?> TypeTag(string messageType) => new("outbox.message.type", messageType);
}
