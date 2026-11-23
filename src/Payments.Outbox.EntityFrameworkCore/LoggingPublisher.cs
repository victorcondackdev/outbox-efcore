using Microsoft.Extensions.Logging;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Publisher that writes messages to the log. Useful for samples and local development.</summary>
public sealed class LoggingPublisher : IOutboxPublisher
{
    private readonly ILogger<LoggingPublisher> _logger;

    /// <summary>Creates the publisher.</summary>
    /// <param name="logger">Logger.</param>
    public LoggingPublisher(ILogger<LoggingPublisher> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
    }

    /// <inheritdoc />
    public Task PublishAsync(OutboxEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        _logger.LogInformation(
            "Published {MessageType} {MessageId} (partition {PartitionKey}, attempt {Attempt}): {Payload}",
            envelope.Type,
            envelope.MessageId,
            envelope.PartitionKey,
            envelope.Attempt,
            envelope.Payload);
        return Task.CompletedTask;
    }
}
