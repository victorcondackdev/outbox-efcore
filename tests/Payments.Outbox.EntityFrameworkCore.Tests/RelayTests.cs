using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Payments.Outbox.EntityFrameworkCore.Tests.Infrastructure;

namespace Payments.Outbox.EntityFrameworkCore.Tests;

public sealed class RelayTests
{
    [Fact]
    public async Task Publishes_pending_messages_and_marks_them_processed()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        var chargeId = Guid.NewGuid();
        var written = await host.WriteAsync(new ChargePaid(chargeId, 1500));

        await using (var context = host.NewContext())
        {
            Assert.Equal(ClaimStrategy.Optimistic, OutboxClaimer.SelectStrategy(context, new OutboxRelayOptions()));
        }

        var handled = await host.Processor.ProcessBatchAsync();

        Assert.Equal(1, handled);
        var envelope = Assert.Single(host.Publisher.Published);
        Assert.Equal(written.Id, envelope.MessageId);
        Assert.Equal(TestTypes.ChargePaidName, envelope.Type);
        Assert.Equal(chargeId.ToString(), envelope.PartitionKey);
        Assert.Equal(1, envelope.Attempt);
        Assert.Equal(OutboxTestHost.Start, envelope.OccurredAt);
        Assert.Equal(new ChargePaid(chargeId, 1500), host.Serializer.Deserialize<ChargePaid>(envelope));

        var stored = await host.GetMessageAsync(written.Id);
        Assert.Equal(OutboxTestHost.Start.UtcDateTime, stored.ProcessedAt);
        Assert.Null(stored.LockedBy);
        Assert.Null(stored.LockedUntil);
        Assert.NotEqual(written.Version, stored.Version);

        Assert.Equal(0, await host.Processor.ProcessBatchAsync());
        Assert.Single(host.Publisher.Published);
    }

    [Fact]
    public async Task Publishes_in_sequence_order_and_respects_batch_size()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync(o => o.BatchSize = 2);
        var ids = new List<Guid>();
        for (var i = 0; i < 5; i++)
        {
            // One charge per message, so every message heads its own partition.
            var row = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), i));
            ids.Add(row.Id);
        }

        Assert.Equal(2, await host.Processor.ProcessBatchAsync());
        Assert.Equal(2, await host.Processor.ProcessBatchAsync());
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        Assert.Equal(0, await host.Processor.ProcessBatchAsync());

        Assert.Equal(ids, host.Publisher.Published.Select(e => e.MessageId).ToList());
    }

    [Fact]
    public async Task Failed_publish_is_retried_with_exponential_backoff()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        var failuresLeft = 2;
        host.Publisher.OnPublish = _ =>
        {
            if (failuresLeft > 0)
            {
                failuresLeft--;
                throw new InvalidOperationException("broker unavailable");
            }

            return Task.CompletedTask;
        };
        var written = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 10));

        // Attempt 1 fails: next attempt after BaseRetryDelay (1s, jitter disabled).
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        var afterFirst = await host.GetMessageAsync(written.Id);
        Assert.Equal(1, afterFirst.Attempts);
        Assert.Equal(OutboxTestHost.Start.UtcDateTime.AddSeconds(1), afterFirst.NextAttemptAt);
        Assert.NotNull(afterFirst.Error);
        Assert.Contains("broker unavailable", afterFirst.Error, StringComparison.Ordinal);
        Assert.Null(afterFirst.LockedBy);

        Assert.Equal(0, await host.Processor.ProcessBatchAsync());

        // Attempt 2 fails: delay doubles to 2s.
        host.Time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        var afterSecond = await host.GetMessageAsync(written.Id);
        Assert.Equal(2, afterSecond.Attempts);
        Assert.Equal(OutboxTestHost.Start.UtcDateTime.AddSeconds(3), afterSecond.NextAttemptAt);

        host.Time.Advance(TimeSpan.FromSeconds(1.5));
        Assert.Equal(0, await host.Processor.ProcessBatchAsync());

        // Attempt 3 succeeds.
        host.Time.Advance(TimeSpan.FromSeconds(0.5));
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        var envelope = Assert.Single(host.Publisher.Published);
        Assert.Equal(3, envelope.Attempt);

        var processed = await host.GetMessageAsync(written.Id);
        Assert.NotNull(processed.ProcessedAt);
        Assert.Null(processed.Error);
    }

    [Fact]
    public async Task Message_is_dead_lettered_after_max_attempts()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync(o => o.MaxAttempts = 3);
        host.Publisher.OnPublish = _ => Task.FromException(new TimeoutException("broker timeout"));
        var written = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 10));

        for (var attempt = 1; attempt <= 3; attempt++)
        {
            Assert.Equal(1, await host.Processor.ProcessBatchAsync());
            host.Time.Advance(TimeSpan.FromMinutes(5));
        }

        var stored = await host.GetMessageAsync(written.Id);
        Assert.Equal(3, stored.Attempts);
        Assert.NotNull(stored.DeadLetteredAt);
        Assert.Null(stored.ProcessedAt);
        Assert.NotNull(stored.Error);
        Assert.StartsWith("System.TimeoutException", stored.Error, StringComparison.Ordinal);

        Assert.Equal(0, await host.Processor.ProcessBatchAsync());
        Assert.Empty(host.Publisher.Published);
    }

    [Fact]
    public async Task Partition_order_is_preserved_across_failures()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        var partitionA = new OutboxMessageOptions { PartitionKey = "A" };
        var a1 = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1), partitionA);
        var a2 = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 2), partitionA);
        var b1 = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 3), new OutboxMessageOptions { PartitionKey = "B" });
        var a3 = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 4), partitionA);

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

        // Only the head of each partition is eligible: a1 (fails) and b1.
        Assert.Equal(2, await host.Processor.ProcessBatchAsync());
        Assert.Equal(new[] { b1.Id }, host.Publisher.Published.Select(e => e.MessageId).ToArray());

        // a1 is backing off, so a2 and a3 must wait behind it.
        Assert.Equal(0, await host.Processor.ProcessBatchAsync());

        host.Time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        Assert.Equal(0, await host.Processor.ProcessBatchAsync());

        var partitionAOrder = host.Publisher.Published
            .Where(e => e.PartitionKey == "A")
            .Select(e => e.MessageId)
            .ToArray();
        Assert.Equal(new[] { a1.Id, a2.Id, a3.Id }, partitionAOrder);
    }

    [Fact]
    public async Task Dead_lettered_message_unblocks_its_partition()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync(o => o.MaxAttempts = 1);
        var partition = new OutboxMessageOptions { PartitionKey = "A" };
        var poison = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1), partition);
        var next = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 2), partition);
        host.Publisher.OnPublish = envelope => envelope.MessageId == poison.Id
            ? Task.FromException(new InvalidOperationException("poison"))
            : Task.CompletedTask;

        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());

        var envelope = Assert.Single(host.Publisher.Published);
        Assert.Equal(next.Id, envelope.MessageId);
    }

    [Fact]
    public async Task Ordering_can_be_disabled()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync(o => o.EnablePartitionOrdering = false);
        var partition = new OutboxMessageOptions { PartitionKey = "A" };
        await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1), partition);
        await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 2), partition);

        Assert.Equal(2, await host.Processor.ProcessBatchAsync());
    }

    [Fact]
    public async Task Claimed_messages_are_invisible_to_other_instances_until_the_lease_expires()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync(o => o.LockDuration = TimeSpan.FromSeconds(30));
        var written = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1));

        // Simulate a relay that claimed the message and crashed before publishing.
        await using (var context = host.NewContext())
        {
            var claimed = await OutboxClaimer.ClaimAsync(
                context,
                new OutboxRelayOptions { InstanceId = "crashed", LockDuration = TimeSpan.FromSeconds(30) },
                OutboxTestHost.Start.UtcDateTime,
                CancellationToken.None);
            Assert.Single(claimed);
        }

        var stored = await host.GetMessageAsync(written.Id);
        Assert.Equal("crashed", stored.LockedBy);

        Assert.Equal(0, await host.Processor.ProcessBatchAsync());

        host.Time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(1, await host.Processor.ProcessBatchAsync());
        Assert.Single(host.Publisher.Published);
    }

    [Fact]
    public async Task Concurrent_instances_never_publish_the_same_message_twice()
    {
        const int messageCount = 60;
        await using var host = await OutboxTestHost.CreateSqliteAsync(o =>
        {
            o.BatchSize = 5;
            o.EnablePartitionOrdering = false;
        });

        await using (var context = host.NewContext())
        {
            for (var i = 0; i < messageCount; i++)
            {
                context.Outbox().Add(new ChargePaid(Guid.NewGuid(), i));
            }

            await context.SaveChangesAsync();
        }

        var relays = new[] { host.CreateProcessor("relay-a"), host.CreateProcessor("relay-b"), host.CreateProcessor("relay-c") };
        await Task.WhenAll(relays.Select(relay => RunUntilDrainedAsync(relay, host.Publisher, messageCount)));

        var published = host.Publisher.Published.Select(e => e.MessageId).ToList();
        Assert.Equal(messageCount, published.Count);
        Assert.Equal(messageCount, published.Distinct().Count());

        var stored = await host.GetMessagesAsync();
        Assert.All(stored, m => Assert.NotNull(m.ProcessedAt));
    }

    [Fact]
    public async Task Cleanup_deletes_processed_messages_past_retention()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync(o => o.ProcessedRetention = TimeSpan.FromDays(7));
        var old = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1));
        await host.Processor.ProcessBatchAsync();

        host.Time.Advance(TimeSpan.FromDays(8));
        var recent = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 2));
        await host.Processor.ProcessBatchAsync();
        var pending = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 3));

        Assert.Equal(1, await host.Processor.CleanupAsync());

        var remaining = (await host.GetMessagesAsync()).Select(m => m.Id).ToList();
        Assert.DoesNotContain(old.Id, remaining);
        Assert.Contains(recent.Id, remaining);
        Assert.Contains(pending.Id, remaining);
    }

    [Fact]
    public async Task Hosted_relay_publishes_in_the_background()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1));

        var service = new OutboxRelayService<TestDbContext>(
            host.Processor,
            Options.Create(new OutboxRelayOptions()),
            host.Time,
            NullLogger<OutboxRelayService<TestDbContext>>.Instance);

        await service.StartAsync(CancellationToken.None);
        try
        {
            var stopwatch = Stopwatch.StartNew();
            while (host.Publisher.Count == 0 && stopwatch.Elapsed < TimeSpan.FromSeconds(10))
            {
                await Task.Delay(20);
            }
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }

        Assert.Single(host.Publisher.Published);
    }

    [Fact]
    public void Invalid_options_are_rejected()
    {
        using var services = new ServiceCollection().BuildServiceProvider();
        var scopeFactory = services.GetRequiredService<IServiceScopeFactory>();

        Assert.Throws<InvalidOperationException>(() => new OutboxProcessor<TestDbContext>(
            scopeFactory,
            Options.Create(new OutboxRelayOptions { BatchSize = 0 }),
            TimeProvider.System,
            NullLogger<OutboxProcessor<TestDbContext>>.Instance));

        Assert.Throws<InvalidOperationException>(() => new OutboxProcessor<TestDbContext>(
            scopeFactory,
            Options.Create(new OutboxRelayOptions { RetryJitter = 1.5 }),
            TimeProvider.System,
            NullLogger<OutboxProcessor<TestDbContext>>.Instance));
    }

    internal static async Task RunUntilDrainedAsync(OutboxProcessor<TestDbContext> relay, InMemoryPublisher publisher, int expected)
    {
        var stopwatch = Stopwatch.StartNew();
        while (publisher.Count < expected && stopwatch.Elapsed < TimeSpan.FromSeconds(60))
        {
            if (await relay.ProcessBatchAsync() == 0)
            {
                await Task.Delay(5);
            }
        }
    }
}
