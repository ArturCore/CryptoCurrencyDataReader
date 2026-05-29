using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookStore
    {
        OrderBook? GetRawOrderBook(string symbol);
        Task<bool> CreateWithSnapshotAsync(OrderBookSnapshot snapshot, CancellationToken cancellationToken = default);
        Task<bool> TryApplyDeltaAsync(OrderBookDelta delta, CancellationToken cancellationToken = default);
        OrderBookSnapshot? TryGetSnapshot(string symbol);
    }
}
