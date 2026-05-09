using Core.Interfaces;
using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookBaseSnapshot
    {
        Task<IResponse<OrderBookSnapshot>> GetSnapshotAsync(
            string symbol, 
            int limit, 
            CancellationToken cancellationToken);
    }
}
