using Core.Configurations;
using Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Core.Services
{
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
