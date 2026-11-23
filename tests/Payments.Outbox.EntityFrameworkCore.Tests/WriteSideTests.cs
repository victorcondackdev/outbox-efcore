using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Payments.Outbox.EntityFrameworkCore.Tests.Infrastructure;

namespace Payments.Outbox.EntityFrameworkCore.Tests;

public sealed class WriteSideTests
{
    [Fact]
    public async Task Message_is_committed_together_with_the_business_change()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();

        await using (var context = host.NewContext())
        {
            var charge = Charge.Create("ch_1", 1500);
            context.Charges.Add(charge);
            context.Outbox().Add(new ChargePaid(charge.Id, 1500));
            await context.SaveChangesAsync();
        }

        await using var verify = host.NewContext();
        Assert.Equal(1, await verify.Charges.CountAsync());
        var message = Assert.Single(await verify.OutboxMessages.ToListAsync());
        Assert.Equal(TestTypes.ChargePaidName, message.Type);
        Assert.Null(message.ProcessedAt);
        Assert.Equal(0, message.Attempts);
        Assert.Equal(OutboxTestHost.Start.UtcDateTime, message.OccurredAt);
    }

    [Fact]
    public async Task Rolled_back_transaction_leaves_no_message()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();

        await using (var context = host.NewContext())
        {
            await using var transaction = await context.Database.BeginTransactionAsync();
            var charge = Charge.Create("ch_1", 1500);
            context.Charges.Add(charge);
            context.Outbox().Add(new ChargePaid(charge.Id, 1500));
            await context.SaveChangesAsync();
            await transaction.RollbackAsync();
        }

        await using var verify = host.NewContext();
        Assert.Equal(0, await verify.Charges.CountAsync());
        Assert.Equal(0, await verify.OutboxMessages.CountAsync());
    }

    [Fact]
    public async Task Failed_save_leaves_no_message()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();

        await using (var context = host.NewContext())
        {
            context.Charges.Add(Charge.Create("ch_dup", 100));
            await context.SaveChangesAsync();
        }

        await using (var context = host.NewContext())
        {
            var duplicate = Charge.Create("ch_dup", 200);
            context.Charges.Add(duplicate);
            context.Outbox().Add(new ChargePaid(duplicate.Id, 200));
            await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync());
        }

        await using var verify = host.NewContext();
        Assert.Equal(0, await verify.OutboxMessages.CountAsync());
    }

    [Fact]
    public async Task Domain_events_are_collected_on_save()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        var charge = Charge.Create("ch_1", 990);

        await using (var context = host.NewContext())
        {
            context.Charges.Add(charge);
            charge.MarkPaid();
            await context.SaveChangesAsync();

            // Events are dequeued: saving again does not duplicate them.
            await context.SaveChangesAsync();
        }

        var message = Assert.Single(await host.GetMessagesAsync());
        Assert.Equal(TestTypes.ChargePaidName, message.Type);
        Assert.Equal(charge.Id.ToString(), message.PartitionKey);
        Assert.Equal(charge.Id.ToString(), message.AggregateId);

        var payload = host.Serializer.Deserialize(message.Type, message.Payload);
        Assert.Equal(new ChargePaid(charge.Id, 990), payload);
    }

    [Fact]
    public async Task Payload_is_json_with_web_defaults()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        var chargeId = Guid.NewGuid();
        var row = await host.WriteAsync(new ChargePaid(chargeId, 42));

        using var document = JsonDocument.Parse(row.Payload);
        Assert.Equal(chargeId, document.RootElement.GetProperty("chargeId").GetGuid());
        Assert.Equal(42, document.RootElement.GetProperty("amountInCents").GetInt64());
    }

    [Fact]
    public async Task Message_options_override_defaults()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        var id = Guid.NewGuid();

        var row = await host.WriteAsync(
            new ChargeRefunded(Guid.NewGuid(), 10),
            new OutboxMessageOptions
            {
                MessageId = id,
                PartitionKey = "merchant-7",
                AggregateId = "refund-1",
                Headers = new Dictionary<string, string> { ["tenant"] = "acme" },
            });

        var stored = await host.GetMessageAsync(id);
        Assert.Equal(row.Sequence, stored.Sequence);
        Assert.Equal("merchant-7", stored.PartitionKey);
        Assert.Equal("refund-1", stored.AggregateId);
        Assert.NotNull(stored.Headers);
        using var headers = JsonDocument.Parse(stored.Headers);
        Assert.Equal("acme", headers.RootElement.GetProperty("tenant").GetString());
    }

    [Fact]
    public async Task Unmapped_message_type_is_rejected()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        await using var context = host.NewContext();

        var exception = Assert.Throws<InvalidOperationException>(() => context.Outbox().Add(new UnmappedEvent("x")));
        Assert.Contains("not mapped", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Outbox_requires_the_interceptor()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>().UseSqlite("Data Source=:memory:").Options;
        using var context = new TestDbContext(options);

        var exception = Assert.Throws<InvalidOperationException>(() => context.Outbox());
        Assert.Contains("UseOutbox", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Message_ids_are_unique_and_version_7()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        var first = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1));
        var second = await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 2));

        Assert.NotEqual(first.Id, second.Id);
        Assert.Equal('7', first.Id.ToString()[14]);
        Assert.Contains(first.Id.ToString().Substring(19, 1), "89ab", StringComparison.Ordinal);
        Assert.True(second.Sequence > first.Sequence);
    }
}
