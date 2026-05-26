using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructure.AggregationSnapshot.Azure
{
    public class AggregatedOrderBookEntity : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public int Depth { get; set; }
        public decimal Price { get; set; }
        public decimal AskVolume { get; set; }
        public decimal BidVolume { get; set; }
        public ETag ETag { get; set; }
    }
}
