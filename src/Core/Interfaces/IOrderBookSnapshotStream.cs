using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookSnapshotStream
    {
        Task StartAsync(Func<OrderBookSnapshot, Task> onSnapshot, CancellationToken ct);
    }
}
