using Payments.Outbox.EntityFrameworkCore;

namespace Payments.Api;

/// <summary>Raised when a charge is created. Contract: payments.charge.created.v1.</summary>
public sealed record ChargeCreated(Guid ChargeId, long AmountInCents, string Currency, DateTimeOffset CreatedAt);

/// <summary>Raised when a charge is captured. Contract: payments.charge.captured.v1.</summary>
public sealed record ChargeCaptured(Guid ChargeId, long AmountInCents, DateTimeOffset CapturedAt);

public sealed class Charge : IHasDomainEvents
{
    private readonly List<object> _domainEvents = new();

    public Guid Id { get; private set; }

    public long AmountInCents { get; private set; }

    public string Currency { get; private set; } = "BRL";

    public string Status { get; private set; } = "authorized";

    public DateTime CreatedAt { get; private set; }

    public DateTime? CapturedAt { get; private set; }

    public static Charge Create(long amountInCents, string currency, DateTimeOffset now)
    {
        var charge = new Charge
        {
            Id = Guid.NewGuid(),
            AmountInCents = amountInCents,
            Currency = currency,
            CreatedAt = now.UtcDateTime,
        };

        // Domain event: collected by the outbox interceptor on SaveChanges.
        charge._domainEvents.Add(new ChargeCreated(charge.Id, amountInCents, currency, now));
        return charge;
    }

    public bool TryCapture(DateTimeOffset now)
    {
        if (Status != "authorized")
        {
            return false;
        }

        Status = "captured";
        CapturedAt = now.UtcDateTime;
        return true;
    }

    public IReadOnlyCollection<object> DequeueDomainEvents()
    {
        var events = _domainEvents.ToArray();
        _domainEvents.Clear();
        return events;
    }
}

public sealed record CreateChargeRequest(long AmountInCents, string? Currency);

public sealed record ChargeResponse(Guid Id, long AmountInCents, string Currency, string Status)
{
    public static ChargeResponse From(Charge charge) => new(charge.Id, charge.AmountInCents, charge.Currency, charge.Status);
}
