using Microsoft.EntityFrameworkCore;
using Payments.Api;
using Payments.Outbox.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddOutbox(types => types
        .Map<ChargeCreated>("payments.charge.created.v1", e => e.ChargeId.ToString())
        .Map<ChargeCaptured>("payments.charge.captured.v1", e => e.ChargeId.ToString()))
    .AddRelay<PaymentsDbContext>(relay =>
    {
        relay.PollingInterval = TimeSpan.FromMilliseconds(500);
        relay.MaxAttempts = 8;
    })
    // Swap for a Kafka / RabbitMQ / Service Bus implementation of IOutboxPublisher.
    .AddPublisher<LoggingPublisher>();

builder.Services.AddDbContext<PaymentsDbContext>((services, options) => options
    .UseSqlite(builder.Configuration.GetConnectionString("Payments") ?? "Data Source=payments.db")
    .UseOutbox(services));

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    scope.ServiceProvider.GetRequiredService<PaymentsDbContext>().Database.EnsureCreated();
}

// The charge and its ChargeCreated event (raised by the entity) commit in one transaction.
app.MapPost("/charges", async (CreateChargeRequest request, PaymentsDbContext db, TimeProvider clock, CancellationToken cancellationToken) =>
{
    if (request.AmountInCents <= 0)
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["amountInCents"] = new[] { "Must be a positive amount in cents." },
        });
    }

    var charge = Charge.Create(request.AmountInCents, request.Currency ?? "BRL", clock.GetUtcNow());
    db.Charges.Add(charge);
    await db.SaveChangesAsync(cancellationToken);
    return Results.Created("/charges/" + charge.Id, ChargeResponse.From(charge));
});

// Explicit outbox write: the state change and the event are saved atomically.
app.MapPost("/charges/{id:guid}/capture", async (Guid id, PaymentsDbContext db, TimeProvider clock, CancellationToken cancellationToken) =>
{
    var charge = await db.Charges.FindAsync(new object[] { id }, cancellationToken);
    if (charge is null)
    {
        return Results.NotFound();
    }

    var now = clock.GetUtcNow();
    if (!charge.TryCapture(now))
    {
        return Results.Conflict(new { error = "Charge is not in an authorized state.", status = charge.Status });
    }

    db.Outbox().Add(new ChargeCaptured(charge.Id, charge.AmountInCents, now));
    await db.SaveChangesAsync(cancellationToken);
    return Results.Ok(ChargeResponse.From(charge));
});

app.MapGet("/charges/{id:guid}", async (Guid id, PaymentsDbContext db, CancellationToken cancellationToken) =>
{
    var charge = await db.Charges.AsNoTracking().SingleOrDefaultAsync(c => c.Id == id, cancellationToken);
    return charge is null ? Results.NotFound() : Results.Ok(ChargeResponse.From(charge));
});

app.Run();
