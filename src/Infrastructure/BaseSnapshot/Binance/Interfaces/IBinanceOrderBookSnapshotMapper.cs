using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;
using Domain;

namespace Infrastructure.SnapshotPublisher.Binance.Interfaces
{
    public interface IBinanceOrderBookSnapshotMapper
    {
        ExternalOrderBookDto MapSdkResult(WebCallResult<BinanceOrderBook> sdkResult);
    }
}
