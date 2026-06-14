using Binance.Net.Interfaces;
using Core.DTO;

namespace Infrastructure.Common.Binance.Mappers
{
    internal class BinanceOrderBookUpdatesMapper
    {
        public ExternalOrderBookEventDto MapSdkResult(IBinanceEventOrderBook sdkResult)
        {
            var bids = sdkResult.Bids
                .Select(b => new ExternalOrderBookLevelDto { Price = b.Price, Volume = b.Quantity })
                .ToList() ?? new List<ExternalOrderBookLevelDto>();

            var asks = sdkResult.Asks
                .Select(a => new ExternalOrderBookLevelDto { Price = a.Price, Volume = a.Quantity })
                .ToList() ?? new List<ExternalOrderBookLevelDto>();

            return new ExternalOrderBookEventDto
            {
                FirstUpdateId = sdkResult.FirstUpdateId,
                LastUpdateId = sdkResult.LastUpdateId,
                Symbol = sdkResult.Symbol,
                Bids = bids,
                Asks = asks
            };
        }
    }
}
