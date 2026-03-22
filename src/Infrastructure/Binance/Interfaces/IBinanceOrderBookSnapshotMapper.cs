using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;
using Domain;

namespace Infrastructure.Binance.Interfaces
{
    public interface IBinanceOrderBookSnapshotMapper
    {
        ExternalOrderBookDto Map(WebCallResult<BinanceOrderBook> sdkResult);
        OrderBookSnapshot Map(ExternalOrderBookDto external);
    }
}
