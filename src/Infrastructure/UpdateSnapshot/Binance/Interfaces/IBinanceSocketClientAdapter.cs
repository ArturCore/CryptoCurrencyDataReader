using Infrastructure.Common.Binance;

namespace Infrastructure.UpdateSnapshot.Binance.Interfaces
{
    public interface IBinanceSocketClientAdapter
    {
        /// <summary>
        /// Subscribe to raw DataEvent<IBinanceFuturesEventOrderBook> updates and receive a subscription handle.
        /// Caller must DisposeAsync() or call UnsubscribeAsync() on the returned OrderBookSubscription when finished.
        /// </summary>
        Task<OrderBookSubscription> SubscribeToOrderBookUpdatesRawAsync(
            IReadOnlyCollection<string> symbols,
            int updateInterval,
            int capacity = 1024,
            CancellationToken cancellationToken = default);
    }
}
