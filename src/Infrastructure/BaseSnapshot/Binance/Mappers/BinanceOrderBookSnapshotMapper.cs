using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;
using Domain;
using Infrastructure.SnapshotPublisher.Binance.Interfaces;

namespace Infrastructure.SnapshotPublisher.Binance.Mappers
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
    }
}
