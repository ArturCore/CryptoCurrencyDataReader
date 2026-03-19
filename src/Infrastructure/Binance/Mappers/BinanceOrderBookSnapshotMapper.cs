using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using Core.Interfaces;
using CryptoExchange.Net.Objects;
using Domain;
using Infrastructure.Binance.Interfaces;

namespace Infrastructure.Binance.Mappers
{
    public class BinanceOrderBookSnapshotMapper : IOrderBookMapper, IBinanceSnapshotResultMapper
    {
        public ExternalOrderBookDto Map(WebCallResult<BinanceOrderBook> sdkResult)
        {
            var bids = sdkResult.Data?.Bids
                .Select(b => new ExternalOrderBookLevelDto { Price = b.Price, Quantity = b.Quantity })
                .ToList() ?? new List<ExternalOrderBookLevelDto>();

            var asks = sdkResult.Data?.Asks
                .Select(a => new ExternalOrderBookLevelDto { Price = a.Price, Quantity = a.Quantity })
                .ToList() ?? new List<ExternalOrderBookLevelDto>();

            return new ExternalOrderBookDto
            {
                Bids = bids,
                Asks = asks
            };
        }

        public OrderBookSnapshot Map(ExternalOrderBookDto callResult)
        {
            return new OrderBookSnapshot
            {
                Bids = callResult.Bids
                    .Select(b => new OrderBookLevel { Price = b.Price, Volume = b.Quantity })
                    .ToList(),
                Asks = callResult.Asks
                    .Select(a => new OrderBookLevel { Price = a.Price, Volume = a.Quantity })
                    .ToList()
            };
        }
    }
}
