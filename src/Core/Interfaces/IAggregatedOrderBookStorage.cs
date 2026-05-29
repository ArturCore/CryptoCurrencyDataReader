using Domain;

namespace Core.Interfaces
{
    public interface IAggregatedOrderBookStorage
    {
        Task SaveAggregatedDataAsync(
            string symbol,
            int depth,
            AggregatedOrderBookEvent aggregatedData,
            CancellationToken cancellationToken);
    }
}
