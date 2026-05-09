using Core.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Inbound
{
    internal class OrderBookUpdateService : BackgroundService
    {
        private readonly ILogger<OrderBookUpdateService> _logger;
        private readonly IOrderBookUpdates _updates;
        private readonly IOrderBookStore _orderBookStore;
        private readonly OrderBookOptions _options;

        public OrderBookUpdateService(
            IOrderBookUpdates updates,
            IOrderBookStore orderBookStore,
            IOptions<OrderBookOptions> options,
            ILogger<OrderBookUpdateService> logger)
        {
            _updates = updates ?? throw new ArgumentNullException(nameof(updates));
            _orderBookStore = orderBookStore ?? throw new ArgumentNullException(nameof(orderBookStore));
            _options = options?.Value ?? new OrderBookOptions { Symbols = new List<string>(), Limit = 0 };
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var symbols = _options?.Symbols ?? Enumerable.Empty<string>();

            if (!symbols.Any())
            {
                _logger.LogWarning("No symbols configured for orderbook updates. Exiting.");
                return;
            }

            _logger.LogInformation("OrderBookUpdateService starting for {Count} symbols.", symbols.Count());

            // Subscribe once and consume the stream of updates.
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await foreach (var response in _updates.StreamOrderBookUpdatesAsync(
                        symbols.ToList(),
                        500,
                        cancellationToken))
                    {
                        if (response is null)
                        {
                            _logger.LogWarning("Stream yielded null response for symbols: {Symbols}.", string.Join(',', symbols));
                            continue;
                        }

                        if (!response.IsSuccess || response.Data is null)
                        {
                            _logger.LogWarning("Failed to get update for symbols: {Symbols}: {Error}", string.Join(',', symbols), response.ErrorMessage ?? "unknown");
                            continue;
                        }

                        var delta = response.Data;

                        // Dispatch by symbol — ApplyDelta is responsible for validation and ordering.
                        var applied = await _orderBookStore.TryApplyDeltaAsync(delta, cancellationToken).ConfigureAwait(false);
                        if (!applied)
                        {
                            _logger.LogWarning("Delta for {Symbol} could not be applied (no book). Symbol: {Symbol}, UpdateId: {UpdateId}", delta.Symbol, delta.Symbol, delta.UpdateId);
                            // Optionally trigger snapshot fetch or buffer delta here.
                        }
                        else
                        {
                            _logger.LogDebug("Applied delta for {Symbol}, UpdateId={UpdateId}", delta.Symbol, delta.UpdateId);
                        }
                    }

                    // If the stream completes (subscription ended), log and retry after delay.
                    _logger.LogWarning("Order book updates stream completed for symbols: {Symbols}. Will retry after delay.", string.Join(',', symbols));
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("OrderBookUpdateService cancellation requested.");
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error while processing updates for symbols: {Symbols}. Will retry after delay.", string.Join(',', symbols));
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        throw;
                    }
                }
            }

            _logger.LogInformation("OrderBookUpdateService stopped.");
        }
    }
}
