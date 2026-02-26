using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Interfaces
{
    public interface IAggregatedOrderBookWriter
    {
        Task WriteAsync(string symbol, AggregationLevel level, AggregatedOrderBookEvent e, CancellationToken ct);
    }
}
