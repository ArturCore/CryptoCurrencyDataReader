using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookUpdatesSource
    {
        IAsyncEnumerable<IResponse<OrderBookDelta>> StreamOrderBookUpdatesAsync(
            IReadOnlyCollection<string> symbols,
            int updateInterval,
            CancellationToken cancellationToken);
    }
}
