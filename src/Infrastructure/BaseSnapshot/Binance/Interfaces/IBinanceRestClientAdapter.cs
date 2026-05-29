using Core.DTO;

namespace Infrastructure.SnapshotPublisher.Binance.Interfaces
{
    public interface IBinanceRestClientAdapter
    {
        Task<ExternalOrderBookDto> GetOrderBookAsync(string symbol, int limit, CancellationToken cancellationToken);
        Task<decimal> GetCurrentPrice(string symbol, CancellationToken cancellationToken);
    }
}
