using Core.Configurations;
using Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Core.Services
{
    /// <summary>
    /// Step 2. Update order book data via socket connection
    /// For symbols with small order book (small amount of activity) we can get base snapshot, aggregate it and save
    /// But symbols with frequent transactions (BTCUSDT, ETHUSDT etc.) Get base order book snapshot (Step 1) can cover max 5000 bids and asks levels
    /// This is not enough to realistically describe market sentiment and use in trading
    /// 
    /// This service connects to currency exchange via sockets
    /// And updates price-volume levels in local object
    /// </summary>
    internal class OrderBookUpdatesService(
        ILogger<OrderBookUpdatesService> _logger,
        IOrderBookUpdatesSource orderBookUpdates,
        IOptions<OrderBookOptions> options,
        IOrderBookStore orderBookStore,
        IOrderBookMapper mapper)
        : IOrderBookUpdates
    {
        public async Task RunOrderBookUpdates(CancellationToken cancellationToken)
        {
            try
            {
                await orderBookUpdates.SubscriteToStreamOrderBookUpdatesAsync(
                    options.Value.Symbols,
                    options.Value.UpdateInterval,
                    async (update, token) =>
                    {
                        var mapped = mapper.MapToDelta(update);

                        await orderBookStore.TryApplyDeltaAsync(mapped, token);

                        _logger.LogDebug("Successfully applied delta for symbol {Symbol}", mapped.Symbol);
                    },
                    cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("OrderBookUpdateService cancellation requested.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while processing updates for symbols: {Symbols}. Will retry after delay.", string.Join(',', options.Value.Symbols));
            }
        }        
    }
}
