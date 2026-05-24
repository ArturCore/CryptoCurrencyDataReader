using Binance.Net.Clients;
using Binance.Net.Interfaces;
using Core.DTO;
using Core.Interfaces;
using Infrastructure.UpdateSnapshot.Binance.Interfaces;
using Microsoft.Extensions.Logging;

namespace Infrastructure.Common.Binance
{
    internal sealed class BinanceSocketClientAdapter(
        BinanceSocketClient client,
        ILogger<BinanceSocketClientAdapter> logger,
        IBinanceOrderBookUpdatesMapper mapper)
        : IOrderBookUpdatesSource
    {
        public async Task SubscriteToStreamOrderBookUpdatesAsync(
            IReadOnlyCollection<string> symbols,
            int updateInterval,
            Func<ExternalOrderBookEventDto, CancellationToken, Task> onUpdateAsync,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(symbols);
            ArgumentNullException.ThrowIfNull(onUpdateAsync);

            var subscribeResult = await client.SpotApi.ExchangeData
                .SubscribeToOrderBookUpdatesAsync(
                    symbols,
                    updateInterval,
                    update =>
                    {
                        _ = HandleUpdateAsync(update.Data, onUpdateAsync, cancellationToken);
                    },
                    cancellationToken);

            if (!subscribeResult.Success)
            {
                throw new InvalidOperationException(
                    $"Failed to subscribe to Binance order book updates: {subscribeResult.Error}");
            }
        }

        private async Task HandleUpdateAsync(
            IBinanceEventOrderBook source,
            Func<ExternalOrderBookEventDto, CancellationToken, Task> onUpdateAsync,
            CancellationToken cancellationToken)
        {
            try
            {
                var mapped = mapper.MapSdkResult(source);

                await onUpdateAsync(mapped, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to handle Binance order book update.");
            }
        }
    }
}
