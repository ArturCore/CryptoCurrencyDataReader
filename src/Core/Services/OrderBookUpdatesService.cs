using Core.Configurations;
using Core.Interfaces;
using Domain;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Core.Services
{
    internal class OrderBookUpdatesService(
        ILogger<OrderBookUpdatesService> _logger)
        : IOrderBookUpdates
    {
        public async Task RunOrderBookUpdates(CancellationToken cancellationToken)
        {
            
        }
        //private readonly ILogger<OrderBookUpdateWorker> _logger;
        //private readonly IOrderBookUpdates _updates;
        //private readonly IOrderBookStore _orderBookStore;
        //private readonly OrderBookOptions _options;

        //public OrderBookUpdateWorker(
        //    IOrderBookUpdates updates,
        //    IOrderBookStore orderBookStore,
        //    IOptions<OrderBookOptions> options,
        //    ILogger<OrderBookUpdateWorker> logger)
        //{
        //    _updates = updates ?? throw new ArgumentNullException(nameof(updates));
        //    _orderBookStore = orderBookStore ?? throw new ArgumentNullException(nameof(orderBookStore));
        //    _options = options?.Value ?? new OrderBookOptions { Symbols = new List<string>(), Limit = 0 };
        //    _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        //}

        //protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        //{
        //    var symbols = _options?.Symbols ?? Enumerable.Empty<string>();

        //    if (!symbols.Any())
        //    {
        //        _logger.LogWarning("No symbols configured for orderbook updates. Exiting.");
        //        return;
        //    }

        //    while (!cancellationToken.IsCancellationRequested)
        //    {
        //        try
        //        {
        //            await foreach (var singleUpdate in _updates.StreamOrderBookUpdatesAsync(
        //                symbols.ToList(),
        //                1000,
        //                cancellationToken))
        //            {
        //                if (!IsValidUpdate(singleUpdate, symbols))
        //                    continue;

        //                var applied = await _orderBookStore.TryApplyDeltaAsync(singleUpdate.Data, cancellationToken).ConfigureAwait(false);

        //                if (!applied)
        //                {
        //                    _logger.LogWarning($"Delta for {singleUpdate.Data.Symbol} could not be applied (no book). UpdateId: {singleUpdate.Data.UpdateId}");
        //                }
        //            }

        //            _logger.LogWarning($"Order book updates stream completed for symbols: {string.Join(',', symbols)}. Will retry after delay.");
        //            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
        //        }
        //        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        //        {
        //            _logger.LogInformation("OrderBookUpdateService cancellation requested.");
        //        }
        //        catch (Exception ex)
        //        {
        //            _logger.LogError(ex, "Error while processing updates for symbols: {Symbols}. Will retry after delay.", string.Join(',', symbols));
        //        }
        //    }

        //    _logger.LogInformation("OrderBookUpdateService stopped.");
        //}

        //private bool IsValidUpdate(
        //    IResponse<OrderBookDelta> response,
        //    IEnumerable<string>? symbols)
        //{
        //    if (response is null)
        //    {
        //        _logger.LogWarning("Stream yielded null response for symbols: {Symbols}.", string.Join(',', symbols));
        //        return false;
        //    }

        //    if (!response.IsSuccess || response.Data is null)
        //    {
        //        _logger.LogWarning("Failed to get update for symbols: {Symbols}: {Error}", string.Join(',', symbols), response.ErrorMessage ?? "unknown");
        //        return false;
        //    }

        //    return true;
        //}
    }
}
