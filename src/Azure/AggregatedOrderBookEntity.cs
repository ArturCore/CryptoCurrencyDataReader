using Azure.Data.Tables;
using Shared;

namespace Azure
{
    public class AggregatedOrderBookEntity : ITableEntity
    {
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
        public string Date { get; set; }
        public double AskVolume { get; set; }
        public double BidVolume { get; set; }
        public double Price { get; set; }

        public AggregatedOrderBookEntity() { }

        public AggregatedOrderBookEntity(string symbol, string timestamp, AggregatedData aggregatedData)
        {
            PartitionKey = symbol;
            RowKey = timestamp;
            Date = aggregatedData.Date;
            AskVolume = (double)aggregatedData.Ask;
            BidVolume = (double)aggregatedData.Bid;
            Price = (double)aggregatedData.Price;
        }
    }
}
