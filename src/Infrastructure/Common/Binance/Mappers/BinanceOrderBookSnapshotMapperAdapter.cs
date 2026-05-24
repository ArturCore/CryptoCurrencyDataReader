using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using Core.Mappers;
using CryptoExchange.Net.Objects;
using Domain;
using Infrastructure.SnapshotPublisher.Binance.Interfaces;

namespace Infrastructure.Common.Binance.Mappers
{
    public class BinanceOrderBookSnapshotMapperAdapter : IBinanceOrderBookSnapshotMapper
    {
        private readonly BinanceOrderBookSnapshotMapper _inner;
        private readonly OrderBookMapper _orderBookMapper;

        public BinanceOrderBookSnapshotMapperAdapter(BinanceOrderBookSnapshotMapper inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public ExternalOrderBookDto MapSdkResult(WebCallResult<BinanceOrderBook> sdkResult)
            => _inner.MapSdkResult(sdkResult);

        public OrderBookSnapshot MapToSnapshot(ExternalOrderBookDto external)
            => _orderBookMapper.MapToSnapshot(external);
    }
}
