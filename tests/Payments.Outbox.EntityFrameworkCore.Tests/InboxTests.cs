using Microsoft.EntityFrameworkCore;
using Payments.Outbox.EntityFrameworkCore.Tests.Infrastructure;

namespace Payments.Outbox.EntityFrameworkCore.Tests;

public sealed class InboxTests
{
    [Fact]
    public async Task Handler_runs_once_per_message()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        var chargeId = Guid.NewGuid();
        var invocations = 0;

        async Task<bool> HandleAsync()
        {
            await using var context = host.NewContext();
            return await context.TryProcessOnceAsync("msg-1", "ledger", _ =>
            {
                invocations++;
                context.LedgerEntries.Add(new LedgerEntry { Id = Guid.NewGuid(), ChargeId = chargeId, AmountInCents = 100 });
                return Task.CompletedTask;
            });
        }

        Assert.True(await HandleAsync());
        Assert.False(await HandleAsync());
        Assert.False(await HandleAsync());

        Assert.Equal(1, invocations);
        await using var verify = host.NewContext();
        Assert.Equal(1, await verify.LedgerEntries.CountAsync());
        var record = Assert.Single(await verify.InboxMessages.ToListAsync());
        Assert.Equal("msg-1", record.MessageId);
        Assert.Equal("ledger", record.Consumer);
        Assert.Equal(OutboxTestHost.Start.UtcDateTime, record.ProcessedAt);
    }

    [Fact]
    public async Task Same_message_is_processed_once_per_consumer()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        await using var context = host.NewContext();

        Assert.True(await context.TryProcessOnceAsync("msg-1", "ledger", _ => Task.CompletedTask));
        Assert.True(await context.TryProcessOnceAsync("msg-1", "notifications", _ => Task.CompletedTask));
        Assert.False(await context.TryProcessOnceAsync("msg-1", "ledger", _ => Task.CompletedTask));
        Assert.True(await context.TryProcessOnceAsync("msg-2", _ => Task.CompletedTask));
        Assert.False(await context.TryProcessOnceAsync("msg-2", _ => Task.CompletedTask));
    }

    [Fact]
    public async Task Failed_handler_records_nothing_and_can_be_retried()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();

        await using (var context = host.NewContext())
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => context.TryProcessOnceAsync("msg-1", _ =>
            {
                context.LedgerEntries.Add(new LedgerEntry { Id = Guid.NewGuid(), AmountInCents = 1 });
                throw new InvalidOperationException("downstream failure");
            }));

            Assert.Empty(context.ChangeTracker.Entries());
        }

        await using (var context = host.NewContext())
        {
            Assert.Equal(0, await context.InboxMessages.CountAsync());
            Assert.Equal(0, await context.LedgerEntries.CountAsync());
            Assert.True(await context.TryProcessOnceAsync("msg-1", _ => Task.CompletedTask));
        }
    }

    [Fact]
    public async Task Handler_can_write_outbox_messages_atomically()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        await using var context = host.NewContext();
        var chargeId = Guid.NewGuid();

        Assert.True(await context.TryProcessOnceAsync("msg-1", _ =>
        {
            context.Outbox().Add(new ChargeRefunded(chargeId, 50));
            return Task.CompletedTask;
        }));

        var message = Assert.Single(await host.GetMessagesAsync());
        Assert.Equal(TestTypes.ChargeRefundedName, message.Type);
    }
}
