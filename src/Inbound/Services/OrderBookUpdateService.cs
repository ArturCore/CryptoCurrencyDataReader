using Core.Interfaces;
using Domain;
using Core.Configurations;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Inbound.Services
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

            _logger.LogInformation($"OrderBookUpdateService starting for {symbols.Count()} symbols.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await foreach (var response in _updates.StreamOrderBookUpdatesAsync(
                        symbols.ToList(),
                        1000,
                        cancellationToken))
                    {
                        if (!IsValidUpdate(response, symbols))
                            continue;

                        var delta = response.Data;

                        var applied = await _orderBookStore.TryApplyDeltaAsync(delta, cancellationToken).ConfigureAwait(false);

                        if (!applied)
                        {
                            _logger.LogWarning($"Delta for {delta.Symbol} could not be applied (no book). UpdateId: {delta.UpdateId}");
                        }
                    }

                    _logger.LogWarning($"Order book updates stream completed for symbols: {string.Join(',', symbols)}. Will retry after delay.");
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("OrderBookUpdateService cancellation requested.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error while processing updates for symbols: {Symbols}. Will retry after delay.", string.Join(',', symbols));
                }
            }

            _logger.LogInformation("OrderBookUpdateService stopped.");
        }

        private bool IsValidUpdate(
            IResponse<OrderBookDelta> response,
            IEnumerable<string>? symbols)
        {
            if (response is null)
            {
                _logger.LogWarning("Stream yielded null response for symbols: {Symbols}.", string.Join(',', symbols));
                return false;
            }

            if (!response.IsSuccess || response.Data is null)
            {
                _logger.LogWarning("Failed to get update for symbols: {Symbols}: {Error}", string.Join(',', symbols), response.ErrorMessage ?? "unknown");
                return false;
            }

            return true;
        }
    }
}
