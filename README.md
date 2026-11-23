# Payments.Outbox.EntityFrameworkCore

[![ci](https://github.com/victorcondackdev/outbox-efcore/actions/workflows/ci.yml/badge.svg)](https://github.com/victorcondackdev/outbox-efcore/actions/workflows/ci.yml)

A transactional outbox (and inbox) for EF Core applications, written for payment services. They
cannot afford to **save a charge and lose its event**, or to **publish an event for a transaction
that rolled back**.

- Events are written to an outbox table **in the same database transaction** as the business
  change, either explicitly (`db.Outbox().Add(...)`) or from domain events raised by entities.
- A background relay publishes them through your `IOutboxPublisher` (Kafka, RabbitMQ, Service Bus, ...)
  with **at-least-once** delivery, exponential backoff with jitter, dead-lettering, optional
  **per-partition ordering** and retention cleanup.
- **Multi-instance safe** on every relational provider (lease plus optimistic compare-and-swap), with
  `FOR UPDATE SKIP LOCKED` on PostgreSQL.
- An **inbox** for idempotent consumers: handler side effects and the "already processed" marker
  commit together.
- `System.Diagnostics.Metrics` and `ActivitySource` instrumentation, with **W3C trace context**
  carried from the request that wrote the event to the publish span.
- No broker client dependencies. `TimeProvider` everywhere, so the behaviour is deterministic under test.

## Why an outbox: the dual-write problem

A handler that commits to the database and then publishes to a broker performs two writes that
cannot share a transaction:

```csharp
await db.SaveChangesAsync();        // 1. charge committed
await producer.ProduceAsync(...);   // 2. process crashes / broker down: event lost forever
```

Reversing the order is worse. The event goes out, the commit fails, and downstream systems (ledger,
notifications, settlement) act on a payment that never existed. Retrying and distributed
transactions do not fix this in general. The outbox does: the event becomes a row in the same
transaction, and a separate relay moves committed rows to the broker.

```mermaid
sequenceDiagram
    autonumber
    participant API as Payments API
    participant DB as Database
    participant Relay as Outbox relay (N instances)
    participant Broker as Broker
    participant Consumer as Consumer (inbox)

    API->>DB: BEGIN; INSERT charge; INSERT outbox row; COMMIT
    Note over API,DB: both or neither: rollback leaves no event
    loop every poll
        Relay->>DB: claim due rows (lease + CAS / SKIP LOCKED)
        Relay->>Broker: publish(envelope with MessageId, traceparent)
        Broker-->>Relay: ack
        Relay->>DB: mark ProcessedAt
    end
    Broker->>Consumer: deliver (possibly more than once)
    Consumer->>DB: BEGIN; side effects; INSERT inbox(MessageId); COMMIT
    Note over Consumer,DB: duplicate MessageId: rejected, handler effects rolled back
```

## Guarantees

| Property | Guarantee |
| --- | --- |
| Atomicity | An outbox row exists if and only if the transaction that wrote it committed. |
| Delivery | **At-least-once.** A crash between the broker ack and `ProcessedAt` re-publishes the message, and so does a publish that outlives its lease. `MessageId` is stable across retries, so consumers de-duplicate on it (see the inbox). |
| Ordering | With `EnablePartitionOrdering` (default), messages that share a `PartitionKey` are published in commit-sequence order. Message *k+1* is not published while *k* is claimed, backing off or unpublished, across all instances. Messages without a key are unordered. A dead-lettered message stops blocking its partition. |
| Ordering scope | Order is defined by the outbox `Sequence` (identity) column, which is assigned at insert time. Two concurrent transactions writing to the same partition can commit in the opposite order to their sequence values. In practice, writes for one aggregate are already serialized by the aggregate's row (optimistic concurrency or locks), so this is the order you want. |
| Failure handling | Exponential backoff (`BaseRetryDelay * 2^(n-1)`, capped by `MaxRetryDelay`) with downward jitter. After `MaxAttempts` the message is dead-lettered (`DeadLetteredAt`, `Error`) and kept for inspection. |

## Quick start

```bash
dotnet add package Payments.Outbox.EntityFrameworkCore
```

**1. Map the tables**

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.AddOutbox();   // OutboxMessages
    modelBuilder.AddInbox();    // InboxMessages (consumers only)
}
```

Add a migration as usual (`dotnet ef migrations add Outbox`).

**2. Register the outbox, the relay and a publisher**

```csharp
builder.Services
    .AddOutbox(types => types
        .Map<ChargePaid>("payments.charge.paid.v1", e => e.ChargeId.ToString())   // partition key
        .Map<ChargeRefunded>("payments.charge.refunded.v1", e => e.ChargeId.ToString()))
    .AddRelay<PaymentsDbContext>(relay =>
    {
        relay.BatchSize = 100;
        relay.MaxAttempts = 10;
        relay.ProcessedRetention = TimeSpan.FromDays(7);
    })
    .AddPublisher<KafkaOutboxPublisher>();

builder.Services.AddDbContext<PaymentsDbContext>((sp, options) => options
    .UseNpgsql(builder.Configuration.GetConnectionString("Payments"))
    .UseOutbox(sp));
```

**3. Write events in the unit of work**

Explicitly:

```csharp
charge.Capture(now);
db.Outbox().Add(new ChargeCaptured(charge.Id, charge.AmountInCents, now));
await db.SaveChangesAsync();   // charge update + outbox row, one transaction
```

Or from domain events: implement `IHasDomainEvents` on the aggregate and the interceptor collects
the events on `SaveChanges`:

```csharp
public sealed class Charge : IHasDomainEvents
{
    private readonly List<object> _events = new();

    public void MarkPaid() { Status = "paid"; _events.Add(new ChargePaid(Id, AmountInCents)); }

    public IReadOnlyCollection<object> DequeueDomainEvents()
    {
        var events = _events.ToArray();
        _events.Clear();
        return events;
    }
}
```

Explicit `BeginTransaction` scopes work as you would expect. If you roll back, the outbox rows go
with the rest of the transaction.

### Why an explicit type registry

The outbox never stores CLR type names, and it never lets data choose the type to instantiate.
Payload-driven polymorphic deserialization (`$type`, assembly-qualified names) is a well-known
gadget vector: anyone who can write a row, or a message, picks the class that gets constructed.
`OutboxTypeRegistry.Map<T>("payments.charge.paid.v1")` is an allow-list fixed at startup, and the
contract name is a versioned wire identifier. A class rename or namespace move therefore never
breaks messages that are already in flight.

## Implementing a publisher

The library ships `LoggingPublisher` (samples) and `InMemoryPublisher` (tests). A production
publisher only returns after the broker has durably accepted the message, and throws otherwise.
Example with Confluent.Kafka (not a dependency of this package):

```csharp
public sealed class KafkaOutboxPublisher : IOutboxPublisher
{
    private readonly IProducer<string, string> _producer;   // singleton, Acks.All, EnableIdempotence

    public KafkaOutboxPublisher(IProducer<string, string> producer) => _producer = producer;

    public async Task PublishAsync(OutboxEnvelope envelope, CancellationToken cancellationToken)
    {
        var headers = new Headers
        {
            { "message-id", Encoding.UTF8.GetBytes(envelope.MessageId.ToString()) },
            { "message-type", Encoding.UTF8.GetBytes(envelope.Type) },
        };
        foreach (var (key, value) in envelope.Headers)   // traceparent / tracestate / custom
        {
            headers.Add(key, Encoding.UTF8.GetBytes(value));
        }

        // Same key => same Kafka partition => broker-side order matches outbox order.
        await _producer.ProduceAsync(
            topic: envelope.Type,
            new Message<string, string>
            {
                Key = envelope.PartitionKey ?? envelope.MessageId.ToString(),
                Value = envelope.Payload,
                Headers = headers,
            },
            cancellationToken);
    }
}
```

## Running several relay instances

Each instance has an `InstanceId`. A claim is a **lease**: `LockedBy` and `LockedUntil` are set
together with a new `Version` (concurrency token).

- **Any relational provider (SQLite, SQL Server, ...):** the relay reads due rows, then claims each
  one with `UPDATE ... SET Version = @new, LockedBy = @me, LockedUntil = @until WHERE Sequence = @s AND Version = @observed`.
  Only one instance can win that compare-and-swap. A loser simply skips the row.
- **PostgreSQL (Npgsql, detected by `Database.ProviderName`):** a short transaction runs
  `SELECT ... ORDER BY Sequence LIMIT @n FOR UPDATE SKIP LOCKED`, leases the rows and commits.
  Competing instances skip each other's rows instead of blocking on them. The SQL is generated from
  the EF model, so custom table names, schemas and naming conventions are honoured.
- Publishing happens **outside** any transaction, so a slow broker never holds database locks.
- If an instance dies, its leases expire after `LockDuration` and another instance takes over.
  Choose a `LockDuration` comfortably above the time it takes to publish one batch.
- Every state transition (claim, processed, retry, dead letter) replaces `Version`, so a stale reader
  can never act on a row that someone else has already advanced.

When the relay finds work it keeps draining without sleeping. It waits `PollingInterval` only when
it finds nothing.

## Idempotent consumers (inbox)

```csharp
var processed = await db.TryProcessOnceAsync(envelope.MessageId.ToString(), "ledger", ct =>
{
    db.LedgerEntries.Add(LedgerEntry.Credit(evt.ChargeId, evt.AmountInCents));
    db.Outbox().Add(new LedgerCredited(evt.ChargeId));   // chained events stay atomic too
    return Task.CompletedTask;
});
// false => duplicate delivery, nothing was done
```

The handler's changes and the `(MessageId, Consumer)` inbox row are saved in one transaction.
When two deliveries race, the primary key rejects the second one, its transaction rolls back, and
the call returns `false`. Inbox rows are kept forever unless you set `InboxRetention`, which bounds
the de-duplication window.

## Observability

| Instrument | Name | Notes |
| --- | --- | --- |
| Counter | `outbox.messages.published` | tag `outbox.message.type` |
| Counter | `outbox.messages.failed` | failed attempts |
| Counter | `outbox.messages.dead_lettered` | |
| Histogram | `outbox.message.lag` (s) | write-to-publish latency |
| Activity | `outbox publish {type}` (Producer) | child of the activity that wrote the message |

When a message is written, the current `Activity` is captured as `traceparent`/`tracestate` in the
row headers. The publish span is started as its child, and its own context goes to the publisher in
`envelope.Headers`, so one trace runs from the HTTP request through the broker to the consumer.

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(OutboxDiagnostics.ActivitySourceName))
    .WithMetrics(m => m.AddMeter(OutboxDiagnostics.MeterName));
```

## Configuration reference (`OutboxRelayOptions`)

| Option | Default | |
| --- | --- | --- |
| `InstanceId` | machine name + random suffix | must be unique per process |
| `PollingInterval` | 1 s | idle sleep |
| `BatchSize` | 100 | messages claimed per poll |
| `LockDuration` | 30 s | lease length |
| `MaxAttempts` | 10 | then dead-letter |
| `BaseRetryDelay` / `MaxRetryDelay` | 1 s / 5 min | exponential backoff bounds |
| `RetryJitter` | 0.2 | fraction of the delay randomized downwards |
| `EnablePartitionOrdering` | true | per-`PartitionKey` ordering |
| `UsePostgresSkipLocked` | true | SKIP LOCKED claim path on Npgsql |
| `ProcessedRetention` | 7 days | cleanup of published rows |
| `DeadLetterRetention` | null (keep) | |
| `InboxRetention` | null (keep) | |
| `CleanupInterval` | 1 h | |

`OutboxProcessor<TContext>` (`ProcessBatchAsync`, `CleanupAsync`) can also be driven directly, for
example from a scheduled job instead of the hosted service.

## Sample

`samples/Payments.Api` is a minimal API on SQLite. `POST /charges` creates a charge whose
`ChargeCreated` domain event goes to the outbox. `POST /charges/{id}/capture` writes
`ChargeCaptured` explicitly. The hosted relay publishes to the console through `LoggingPublisher`.

```bash
dotnet run --project samples/Payments.Api
curl -X POST localhost:5000/charges -H 'content-type: application/json' -d '{"amountInCents":1500}'
```

## Tests

- SQLite (real relational engine, temporary file per test): atomic write and rollback, domain events,
  publish and mark, backoff under `FakeTimeProvider`, dead-lettering, partition ordering, lease
  expiry, three concurrent relays without double publication, cleanup, inbox de-duplication and
  trace propagation.
- PostgreSQL (CI `postgres:16` service, `OUTBOX_PG` connection string; skipped when unset):
  the SKIP LOCKED path, rows locked by another transaction, four competing relays over 300 messages
  with per-partition order checks, and concurrent inbox deliveries.

```bash
dotnet test
OUTBOX_PG="Host=localhost;Username=postgres;Password=postgres" dotnet test --filter Category=Postgres
```

## Related

- [payments-service-go](https://github.com/victorcondackdev/payments-service-go): a payments service in Go built on the same reliability patterns.

## License

[MIT](LICENSE)
