using System.Text.Json;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Write-side outbox options.</summary>
public sealed class OutboxOptions
{
    /// <summary>
    /// Serializer options used for message payloads. Defaults to <see cref="JsonSerializerDefaults.Web"/>
    /// (camelCase, case-insensitive reads).
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// When <see langword="true"/> (default), the W3C trace context of <see cref="System.Diagnostics.Activity.Current"/>
    /// is stored in the message headers so that publishing continues the originating trace.
    /// </summary>
    public bool CaptureTraceContext { get; set; } = true;
}
