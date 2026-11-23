using Microsoft.EntityFrameworkCore;
using Payments.Outbox.EntityFrameworkCore.Tests.Infrastructure;

namespace Payments.Outbox.EntityFrameworkCore.Tests;

/// <summary>
/// Runs against a real PostgreSQL server (CI: postgres:16 service container, OUTBOX_PG set).
/// Each test creates and drops its own database.
/// </summary>
[Trait("Category", "Postgres")]
public sealed class PostgresIntegrationTests
{
    [PostgresFact]
    public async Task Uses_skip_locked_and_publishes()
    {
        await using var host = await OutboxTestHost.CreatePostgresAsync();
        await using (var context = host.NewContext())
        {
            Assert.Equal(ClaimStrategy.SkipLocked, OutboxClaimer.SelectStrategy(context, new OutboxRelayOptions()));
        }

        var written = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1500));

        Assert.Equal(1, await host.Processor.ProcessBatchAsync());

        var envelope = Assert.Single(host.Publisher.Published);
        Assert.Equal(written.Id, envelope.MessageId);
        var stored = await host.GetMessageAsync(written.Id);
        Assert.NotNull(stored.ProcessedAt);
        Assert.Null(stored.LockedBy);
    }

    [PostgresFact]
    public async Task Rows_locked_by_another_transaction_are_skipped()
    {
        await using var host = await OutboxTestHost.CreatePostgresAsync();
        var locked = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1));
        await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 2));
        await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 3));

        await using (var blocker = host.NewContext())
        {
            await using var transaction = await blocker.Database.BeginTransactionAsync();
            await blocker.Database.ExecuteSqlRawAsync(
                "SELECT 1 FROM \"OutboxMessages\" WHERE \"Id\" = {0} FOR UPDATE",
                locked.Id);

            // The relay must not wait for the row lock: it skips the row and publishes the rest.
            Assert.Equal(2, await host.Processor.ProcessBatchAsync());
            Assert.DoesNotContain(host.Publisher.Published, e => e.MessageId == locked.Id);

            await transaction.RollbackAsync();
        }

        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        Assert.Contains(host.Publisher.Published, e => e.MessageId == locked.Id);
    }

    [PostgresFact]
    public async Task Concurrent_instances_never_publish_the_same_message_twice()
    {
        const int messageCount = 300;
        await using var host = await OutboxTestHost.CreatePostgresAsync(o => o.BatchSize = 10);

        await using (var context = host.NewContext())
        {
            for (var i = 0; i < messageCount; i++)
            {
                // A handful of partitions keeps ordering checks in play while instances compete.
                context.Outbox().Add(new ChargePaid(Guid.NewGuid(), i), new OutboxMessageOptions { PartitionKey = "p" + (i % 7) });
            }

            await context.SaveChangesAsync();
        }

        var relays = Enumerable.Range(0, 4).Select(i => host.CreateProcessor("relay-" + i)).ToArray();
        await Task.WhenAll(relays.Select(relay => RelayTests.RunUntilDrainedAsync(relay, host.Publisher, messageCount)));

        var published = host.Publisher.Published.Select(e => e.MessageId).ToList();
        Assert.Equal(messageCount, published.Count);
        Assert.Equal(messageCount, published.Distinct().Count());

        // Per-partition order holds even with competing instances.
        var stored = await host.GetMessagesAsync();
        var sequenceById = stored.ToDictionary(m => m.Id, m => m.Sequence);
        foreach (var partition in host.Publisher.Published.GroupBy(e => e.PartitionKey))
        {
            var sequences = partition.Select(e => sequenceById[e.MessageId]).ToList();
            Assert.Equal(sequences.OrderBy(s => s).ToList(), sequences);
        }
    }

    [PostgresFact]
    public async Task Partition_order_is_preserved_across_failures()
    {
        await using var host = await OutboxTestHost.CreatePostgresAsync();
        var partition = new OutboxMessageOptions { PartitionKey = "A" };
        var a1 = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1), partition);
        var a2 = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 2), partition);

        var failedOnce = false;
        host.Publisher.OnPublish = envelope =>
        {
            if (envelope.MessageId == a1.Id && !failedOnce)
            {
                failedOnce = true;
                throw new InvalidOperationException("transient");
            }

            return Task.CompletedTask;
        };

        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        Assert.Equal(0, await host.Processor.ProcessBatchAsync());

        host.Time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());

        Assert.Equal(new[] { a1.Id, a2.Id }, host.Publisher.Published.Select(e => e.MessageId).ToArray());
    }

    [PostgresFact]
    public async Task Inbox_deduplicates_concurrent_deliveries()
    {
        await using var host = await OutboxTestHost.CreatePostgresAsync();
        var chargeId = Guid.NewGuid();

        async Task<bool> DeliverAsync()
        {
            await using var context = host.NewContext();
            return await context.TryProcessOnceAsync("msg-1", "ledger", async cancellationToken =>
            {
                context.LedgerEntries.Add(new LedgerEntry { Id = Guid.NewGuid(), ChargeId = chargeId, AmountInCents = 100 });
                await Task.Delay(50, cancellationToken);
            });
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 5).Select(_ => DeliverAsync()));

        Assert.Single(results, processed => processed);
        await using var verify = host.NewContext();
        Assert.Equal(1, await verify.LedgerEntries.CountAsync());
        Assert.Equal(1, await verify.InboxMessages.CountAsync());
    }

    [PostgresFact]
    public async Task Cleanup_runs_on_postgres()
    {
        await using var host = await OutboxTestHost.CreatePostgresAsync(o => o.InboxRetention = TimeSpan.FromDays(1));
        await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1));
        await host.Processor.ProcessBatchAsync();
        await using (var context = host.NewContext())
        {
            await context.TryProcessOnceAsync("msg-1", _ => Task.CompletedTask);
        }

        host.Time.Advance(TimeSpan.FromDays(8));

        Assert.Equal(2, await host.Processor.CleanupAsync());
        Assert.Empty(await host.GetMessagesAsync());
    }
}
