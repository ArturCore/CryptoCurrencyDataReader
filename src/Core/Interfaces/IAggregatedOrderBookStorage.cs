using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
