using Binance.Net.Interfaces;
using Core.DTO;
using Domain;

namespace Infrastructure.UpdateSnapshot.Binance.Interfaces
{
    public interface IBinanceOrderBookUpdatesMapper
    {
        ExternalOrderBookEventDto MapSdkResult(IBinanceEventOrderBook sdkResult);
        OrderBookDelta MapToDelta(ExternalOrderBookEventDto external);
    }
}
