using Microsoft.Extensions.Options;
using Payments.Outbox.EntityFrameworkCore.Tests.Infrastructure;

namespace Payments.Outbox.EntityFrameworkCore.Tests;

public sealed class UnitTests
{
    [Fact]
    public void Registry_rejects_duplicate_names_and_types()
    {
        var registry = new OutboxTypeRegistry().Map<ChargePaid>("payments.charge.paid.v1");

        Assert.Throws<InvalidOperationException>(() => registry.Map<ChargeRefunded>("payments.charge.paid.v1"));
        Assert.Throws<InvalidOperationException>(() => registry.Map<ChargePaid>("payments.charge.paid.v2"));
        Assert.Throws<ArgumentException>(() => registry.Map<ChargeRefunded>(" "));
    }

    [Fact]
    public void Registry_resolves_both_directions()
    {
        var registry = new OutboxTypeRegistry();
        TestTypes.Register(registry);

        Assert.True(registry.TryGetTypeName(typeof(ChargePaid), out var name));
        Assert.Equal(TestTypes.ChargePaidName, name);
        Assert.True(registry.TryGetType(TestTypes.ChargeRefundedName, out var type));
        Assert.Equal(typeof(ChargeRefunded), type);
        Assert.False(registry.TryGetType("System.Diagnostics.Process", out _));
        Assert.Equal(2, registry.TypeNames.Count);
    }

    [Fact]
    public void Serializer_refuses_unmapped_contract_names()
    {
        var registry = new OutboxTypeRegistry();
        TestTypes.Register(registry);
        var serializer = new OutboxSerializer(registry, Options.Create(new OutboxOptions()));

        Assert.Throws<InvalidOperationException>(() => serializer.Deserialize("System.Object", "{}"));

        var payload = serializer.Serialize(new ChargePaid(Guid.Empty, 7), out var typeName);
        Assert.Equal(TestTypes.ChargePaidName, typeName);
        Assert.Equal(new ChargePaid(Guid.Empty, 7), serializer.Deserialize(typeName, payload));
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(4, 8)]
    [InlineData(10, 60)]
    [InlineData(1000, 60)]
    public void Backoff_doubles_up_to_the_cap(int failedAttempts, int expectedSeconds)
    {
        var options = new OutboxRelayOptions
        {
            BaseRetryDelay = TimeSpan.FromSeconds(1),
            MaxRetryDelay = TimeSpan.FromSeconds(60),
            RetryJitter = 0,
        };

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), RetryPolicy.ComputeDelay(failedAttempts, options, 0.99));
    }

    [Fact]
    public void Jitter_only_shortens_the_delay()
    {
        var options = new OutboxRelayOptions
        {
            BaseRetryDelay = TimeSpan.FromSeconds(10),
            MaxRetryDelay = TimeSpan.FromSeconds(60),
            RetryJitter = 0.5,
        };

        Assert.Equal(TimeSpan.FromSeconds(10), RetryPolicy.ComputeDelay(1, options, 0));
        Assert.Equal(TimeSpan.FromSeconds(5), RetryPolicy.ComputeDelay(1, options, 1));
        var sampled = RetryPolicy.ComputeDelay(1, options, 0.5);
        Assert.InRange(sampled, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void Ids_are_time_ordered()
    {
        var earlier = OutboxIds.NewId(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var later = OutboxIds.NewId(new DateTimeOffset(2026, 1, 1, 0, 0, 1, TimeSpan.Zero));

        Assert.True(string.CompareOrdinal(earlier.ToString(), later.ToString()) < 0);
        Assert.Equal('7', earlier.ToString()[14]);
    }
}
