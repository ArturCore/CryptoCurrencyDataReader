using Core.Configurations;
using Core.Interfaces;
using Domain;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Core.Services
{
    internal class OrderBookAggregationService(
        ILogger<OrderBookAggregationService> logger,
        IOptions<OrderBookOptions> options,
        IOrderBookStore orderBookStore,
        IBaseOrderBookSource orderBookSource,
        IAggregatedOrderBookStorage aggregatedOrderBookStorage)
        : IOrderBookAggregation
    {
        public async Task Execute(CancellationToken cancellationToken)
        {
            foreach(string symbol in options.Value.Symbols)
            {
                OrderBookSnapshot? snapshot = orderBookStore.TryGetSnapshot(symbol);

                if (snapshot == null)
                {
                    logger.LogWarning("Snapshot for {Symbol} is empty", symbol);
                    continue;
                }

                var currentPairPrice = await orderBookSource.GetSymbolPrice(symbol, cancellationToken);
                if (!currentPairPrice.IsSuccess)
                {
                    logger.LogWarning("Error while getting {Symbol} price", symbol);
                    continue;
                }

                await ProcessSnapshot(symbol, snapshot, currentPairPrice.Data, cancellationToken);

                logger.LogInformation("Aggregation for symbol {Symbol} succeed", symbol);
            }
        }

        private async Task ProcessSnapshot(string symbol, OrderBookSnapshot snapshot, decimal currentPairPrice, CancellationToken cancellationToken)
        {
            foreach (int aggregationLevel in options.Value.AggregationLevels)
            {
                AggregatedOrderBookEvent aggregatedSnapshot = snapshot.Aggregate(symbol, aggregationLevel, currentPairPrice);

                await aggregatedOrderBookStorage.SaveAggregatedDataAsync(symbol, aggregationLevel, aggregatedSnapshot, cancellationToken);
            }
        }
    }
}
