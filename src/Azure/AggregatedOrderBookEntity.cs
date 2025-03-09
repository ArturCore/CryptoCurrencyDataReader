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
        public string Symbol { get; set; }
        public string Date { get; set; }
        public double AskVolume { get; set; }
        public double BidVolume { get; set; }
        public double Price { get; set; }
        public double Amount { get; set; }
        public double Delta { get; set; }
        public double A7 { get; set; }
        public int DepthPercentage { get; set; }

        public AggregatedOrderBookEntity() { }

        public AggregatedOrderBookEntity(string symbol, string timestamp, AggregatedData aggregatedData)
        {
            PartitionKey = symbol;
            RowKey = timestamp;
            Symbol = symbol;
            Date = aggregatedData.Date;
            AskVolume = (double)aggregatedData.Ask;
            BidVolume = (double)aggregatedData.Bid;
            Price = (double)aggregatedData.Price;
            Amount = (double)aggregatedData.Amount;
            Delta = (double)aggregatedData.Delta;
            A7 = (double)aggregatedData.A7;
            DepthPercentage = aggregatedData.Depth;
        }
    }
}
