using Binance.Net.Interfaces;
using Core.DTO;
using Core.Mappers;
using Domain;
using Infrastructure.UpdateSnapshot.Binance.Interfaces;

namespace Infrastructure.Common.Binance.Mappers
{
    internal class BinanceOrderBookUpdatesMapperAdapter : IBinanceOrderBookUpdatesMapper
    {
        private readonly BinanceOrderBookUpdatesMapper _inner;
        private readonly OrderBookMapper _orderBookMapper;

        public BinanceOrderBookUpdatesMapperAdapter(BinanceOrderBookUpdatesMapper inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public ExternalOrderBookEventDto MapSdkResult(IBinanceEventOrderBook sdkResult)
            => _inner.MapSdkResult(sdkResult);

        public OrderBookDelta MapToDelta(ExternalOrderBookEventDto external)
            => _orderBookMapper.MapToDelta(external);
    }
}
