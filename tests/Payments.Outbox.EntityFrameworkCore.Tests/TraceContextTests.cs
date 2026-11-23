using System.Diagnostics;
using Payments.Outbox.EntityFrameworkCore.Tests.Infrastructure;

namespace Payments.Outbox.EntityFrameworkCore.Tests;

public sealed class TraceContextTests
{
    [Fact]
    public async Task Publish_activity_continues_the_trace_that_wrote_the_message()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == OutboxDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);

        await using var host = await OutboxTestHost.CreateSqliteAsync();

        ActivityTraceId traceId;
        ActivitySpanId writerSpanId;
        using (var request = new Activity("POST /charges"))
        {
            request.SetIdFormat(ActivityIdFormat.W3C);
            request.TraceStateString = "vendor=1";
            request.Start();
            traceId = request.TraceId;
            writerSpanId = request.SpanId;
            await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1));
        }

        var stored = Assert.Single(await host.GetMessagesAsync());
        Assert.NotNull(stored.Headers);
        Assert.Contains(traceId.ToHexString(), stored.Headers, StringComparison.Ordinal);

        Activity? publishActivity = null;
        host.Publisher.OnPublish = _ =>
        {
            publishActivity = Activity.Current;
            return Task.CompletedTask;
        };

        await host.Processor.ProcessBatchAsync();

        Assert.NotNull(publishActivity);
        Assert.Equal(OutboxDiagnostics.ActivitySourceName, publishActivity.Source.Name);
        Assert.Equal(ActivityKind.Producer, publishActivity.Kind);
        Assert.Equal(traceId, publishActivity.TraceId);
        Assert.Equal(writerSpanId, publishActivity.ParentSpanId);

        var envelope = Assert.Single(host.Publisher.Published);
        Assert.True(ActivityContext.TryParse(envelope.Headers["traceparent"], envelope.Headers["tracestate"], out var propagated));
        Assert.Equal(traceId, propagated.TraceId);
        Assert.Equal(publishActivity.SpanId, propagated.SpanId);
        Assert.Equal("vendor=1", propagated.TraceState);
    }

    [Fact]
    public async Task No_trace_headers_without_an_ambient_activity()
    {
        await using var host = await OutboxTestHost.CreateSqliteAsync();
        var previous = Activity.Current;
        Activity.Current = null;
        try
        {
            await host.WriteAsync(new ChargePaid(Guid.NewGuid(), 1));
        }
        finally
        {
            Activity.Current = previous;
        }

        var stored = Assert.Single(await host.GetMessagesAsync());
        Assert.Null(stored.Headers);
    }
}
