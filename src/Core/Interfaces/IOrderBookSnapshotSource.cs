using Core.Interfaces;
using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookSnapshotSource
    {
        Task<IResponse<OrderBookSnapshot>> GetSnapshotAsync(string symbol, int limit, CancellationToken cancellationToken);
    }
}
