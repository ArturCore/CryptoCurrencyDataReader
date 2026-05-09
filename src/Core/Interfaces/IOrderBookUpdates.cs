using Domain;
using System.Runtime.CompilerServices;

namespace Core.Interfaces
{
    public interface IOrderBookUpdates
    {
        Task<IResponse<OrderBookDelta>> GetOrderBookUpdatesAsync(
            IReadOnlyCollection<string> symbols,
            int updateInterval,
            CancellationToken cancellationToken
        );

        /// <summary>
        /// Stream order book updates as they arrive. Consumer should iterate the returned IAsyncEnumerable
        /// to receive deltas continuously. The provided cancellation token will be used to cancel the subscription.
        /// </summary>
        IAsyncEnumerable<IResponse<OrderBookDelta>> StreamOrderBookUpdatesAsync(
            IReadOnlyCollection<string> symbols,
            int updateInterval,
            [EnumeratorCancellation] CancellationToken cancellationToken);
    }
}
