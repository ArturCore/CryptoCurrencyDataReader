using Core.Interfaces;
using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
