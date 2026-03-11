using Core.Interfaces;
using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookSnapshotSource
    {
        IResponse<Task<OrderBookSnapshot>> GetSnapshotAsync(string symbol, int limit, CancellationToken cancellationToken);
    }
}
