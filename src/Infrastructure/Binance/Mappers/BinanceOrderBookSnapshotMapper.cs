using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;
using Domain;
using Infrastructure.Binance.Interfaces;

namespace Infrastructure.Binance.Mappers
{
    public class BinanceOrderBookSnapshotMapper : IBinanceOrderBookSnapshotMapper
    {
        public ExternalOrderBookDto MapSdkResult(WebCallResult<BinanceOrderBook> sdkResult)
        {
            var bids = sdkResult.Data?.Bids
                .Select(b => new ExternalOrderBookLevelDto { Price = b.Price, Volume = b.Quantity })
                .ToList() ?? new List<ExternalOrderBookLevelDto>();

            var asks = sdkResult.Data?.Asks
                .Select(a => new ExternalOrderBookLevelDto { Price = a.Price, Volume = a.Quantity })
                .ToList() ?? new List<ExternalOrderBookLevelDto>();

            return new ExternalOrderBookDto
            {
                Bids = bids,
                Asks = asks
            };
        }

        public OrderBookSnapshot MapToSnapshot(ExternalOrderBookDto external)
        {
            return new OrderBookSnapshot
            {
                Bids = external.Bids
                    .Select(b => new OrderBookLevel { Price = b.Price, Volume = b.Volume })
                    .ToList(),
                Asks = external.Asks
                    .Select(a => new OrderBookLevel { Price = a.Price, Volume = a.Volume })
                    .ToList()
            };
        }
    }
}
