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
        public OrderBookSnapshot Map(ExternalOrderBookDto callResult)
        {
            throw new NotImplementedException();
        }

        public ExternalOrderBookDto Map(WebCallResult<BinanceOrderBook> sdkResult)
        {
            throw new NotImplementedException();
        }
    }
}
