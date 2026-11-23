using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Payments.Outbox.EntityFrameworkCore;

/// <summary>
/// Hosted service that polls the outbox of <typeparamref name="TContext"/>: it drains due messages
/// batch after batch, sleeps for <see cref="OutboxRelayOptions.PollingInterval"/> when idle, and runs
/// the retention cleanup every <see cref="OutboxRelayOptions.CleanupInterval"/>.
/// </summary>
/// <typeparam name="TContext">The context that owns the outbox table.</typeparam>
public sealed class OutboxRelayService<TContext> : BackgroundService
    where TContext : DbContext
{
    private readonly OutboxProcessor<TContext> _processor;
    private readonly OutboxRelayOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    /// <summary>Creates the service.</summary>
    /// <param name="processor">The processor.</param>
    /// <param name="options">Relay options.</param>
    /// <param name="timeProvider">Clock used for polling delays.</param>
    /// <param name="logger">Logger.</param>
    public OutboxRelayService(
        OutboxProcessor<TContext> processor,
        IOptions<OutboxRelayOptions> options,
        TimeProvider timeProvider,
        ILogger<OutboxRelayService<TContext>> logger)
    {
        ArgumentNullException.ThrowIfNull(processor);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);
        _processor = processor;
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox relay {InstanceId} started", _processor.InstanceId);
        var nextCleanup = _timeProvider.GetUtcNow();

        while (!stoppingToken.IsCancellationRequested)
        {
            var handled = 0;
            try
            {
                if (_timeProvider.GetUtcNow() >= nextCleanup)
                {
                    nextCleanup = _timeProvider.GetUtcNow() + _options.CleanupInterval;
                    var deleted = await _processor.CleanupAsync(stoppingToken);
                    if (deleted > 0)
                    {
                        _logger.LogInformation("Outbox cleanup deleted {Count} rows", deleted);
                    }
                }

                handled = await _processor.ProcessBatchAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Outbox relay {InstanceId} iteration failed", _processor.InstanceId);
            }

            if (handled > 0)
            {
                // More work may be waiting (full batch, or the next message of an ordered partition).
                continue;
            }

            try
            {
                await Task.Delay(_options.PollingInterval, _timeProvider, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _logger.LogInformation("Outbox relay {InstanceId} stopped", _processor.InstanceId);
    }
}
