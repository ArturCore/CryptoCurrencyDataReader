using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookDeltaStream
    {
        Task StartAsync(string symbol, Func<OrderBookDelta, Task> onDelta, CancellationToken cancellationToken);
    }
}
