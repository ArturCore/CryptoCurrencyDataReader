namespace Domain
{
    public class OrderBookSnapshot
    {
        public string Symbol { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Bids { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Asks { get; set; }

        public AggregatedOrderBookEvent Aggregate(string symbol, int aggregationLevel, decimal price)
        {
            decimal priceRange = price * (aggregationLevel / 100m);

            var depthBids = Bids.Where(b => b.Price >= price - priceRange);
            decimal bidVolume = depthBids.Sum(b => b.Volume);

            var depthAsks = Asks.Where(a => a.Price <= price + priceRange);
            decimal askVolume = depthAsks.Sum(a => a.Volume);

            return new AggregatedOrderBookEvent { 
                BidVolume = bidVolume,
                AskVolume = askVolume,
                Price = price,
                Timestamp = DateTime.UtcNow
            };
        }
    }
}
