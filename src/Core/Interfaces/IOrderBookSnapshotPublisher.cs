using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookSnapshotPublisher
    {
        Task PublishAsync(OrderBookSnapshot snapshot, CancellationToken ct);
    }
}
