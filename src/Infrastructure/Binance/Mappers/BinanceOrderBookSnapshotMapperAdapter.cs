using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;
using Domain;
using Infrastructure.Binance.Interfaces;

namespace Infrastructure.Binance.Mappers
{
    public class BinanceOrderBookSnapshotMapperAdapter : IBinanceOrderBookSnapshotMapper
    {
        private readonly BinanceOrderBookSnapshotMapper _inner;

        public BinanceOrderBookSnapshotMapperAdapter(BinanceOrderBookSnapshotMapper inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public ExternalOrderBookDto Map(WebCallResult<BinanceOrderBook> sdkResult)
            => _inner.Map(sdkResult);

        public OrderBookSnapshot Map(ExternalOrderBookDto external)
            => _inner.Map(external);
    }
}
