using System.Collections.Concurrent;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>Thread-safe publisher that records messages in memory. Intended for tests.</summary>
public sealed class InMemoryPublisher : IOutboxPublisher
{
    private readonly ConcurrentQueue<OutboxEnvelope> _published = new();

    /// <summary>
    /// Optional hook invoked before a message is recorded. Throw (or return a faulted task) to
    /// simulate a broker failure; the message is then not recorded.
    /// </summary>
    public Func<OutboxEnvelope, Task>? OnPublish { get; set; }

    /// <summary>Snapshot of the successfully published messages, in publication order.</summary>
    public IReadOnlyList<OutboxEnvelope> Published => _published.ToArray();

    /// <summary>Number of successfully published messages.</summary>
    public int Count => _published.Count;

    /// <inheritdoc />
    public async Task PublishAsync(OutboxEnvelope envelope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        cancellationToken.ThrowIfCancellationRequested();
        var hook = OnPublish;
        if (hook is not null)
        {
            await hook(envelope);
        }

        _published.Enqueue(envelope);
    }

    /// <summary>Forgets all recorded messages.</summary>
    public void Clear() => _published.Clear();
}
