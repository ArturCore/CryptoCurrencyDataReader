using Domain;

namespace Core.Interfaces
{
    public interface IAggregatedOrderBookWriter
    {
        Task WriteAsync(string symbol, AggregationLevel level, AggregatedOrderBookEvent e, CancellationToken ct);
    }
}
