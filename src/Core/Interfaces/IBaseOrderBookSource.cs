using Domain;

namespace Core.Interfaces
{
    public interface IBaseOrderBookSource
    {
        Task<IResponse<OrderBookSnapshot>> GetSnapshotAsync(
            string symbol,
            int limit,
            CancellationToken cancellationToken);
        Task<IResponse<decimal>> GetSymbolPrice(
            string symbol,
            CancellationToken cancellationToken);
    }
}
