namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Records that a consumer has processed a message. Written by
/// <see cref="InboxDbContextExtensions"/> in the same transaction as the consumer's own changes, which makes the consumer idempotent.
/// </summary>
public sealed class InboxMessage
{
    /// <summary>Identifier of the incoming message (for example the outbox <see cref="OutboxMessage.Id"/>).</summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>Logical consumer name. The same message may be processed once per consumer.</summary>
    public string Consumer { get; set; } = string.Empty;

    /// <summary>UTC instant at which the message was processed.</summary>
    public DateTime ProcessedAt { get; set; }
}
