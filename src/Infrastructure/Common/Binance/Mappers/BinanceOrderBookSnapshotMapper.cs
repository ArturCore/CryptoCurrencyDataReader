using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;

namespace Infrastructure.Common.Binance.Mappers
{
    public class BinanceOrderBookSnapshotMapper
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
                Symbol = sdkResult.Data?.Symbol,
                Bids = bids,
                Asks = asks
            };
        }
    }
}
