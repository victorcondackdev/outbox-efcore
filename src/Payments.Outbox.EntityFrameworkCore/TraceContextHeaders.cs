using System.Diagnostics;
using System.Text.Json;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>W3C trace context (traceparent/tracestate) stored in outbox message headers.</summary>
internal static class TraceContextHeaders
{
    internal const string TraceParent = "traceparent";
    internal const string TraceState = "tracestate";

    internal static void Inject(Activity? activity, IDictionary<string, string> headers)
    {
        if (activity is null || activity.IdFormat != ActivityIdFormat.W3C || activity.Id is null)
        {
            return;
        }

        headers[TraceParent] = activity.Id;

        // tracestate is vendor data carried along the trace; keep an inherited value when the
        // activity does not expose one of its own.
        if (!string.IsNullOrEmpty(activity.TraceStateString))
        {
            headers[TraceState] = activity.TraceStateString;
        }
    }

    internal static ActivityContext Extract(IReadOnlyDictionary<string, string> headers)
    {
        if (!headers.TryGetValue(TraceParent, out var traceParent))
        {
            return default;
        }

        headers.TryGetValue(TraceState, out var traceState);
        return ActivityContext.TryParse(traceParent, traceState, out var context) ? context : default;
    }

    internal static string? Serialize(Dictionary<string, string> headers) =>
        headers.Count == 0 ? null : JsonSerializer.Serialize(headers);

    internal static Dictionary<string, string> Parse(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }

        var parsed = JsonSerializer.Deserialize<Dictionary<string, string>>(json);
        return parsed is null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : new Dictionary<string, string>(parsed, StringComparer.Ordinal);
    }
}
