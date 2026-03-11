using Core.Interfaces;
using Domain;

namespace Infrastructure.Azure
{
    internal class AzureAggregatedOrderBookWriter : IAggregatedOrderBookWriter
    {
        public Task WriteAsync(string symbol, AggregationLevel level, AggregatedOrderBookEvent e, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}
