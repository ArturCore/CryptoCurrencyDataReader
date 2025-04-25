namespace Shared
{
    public class OrderBookAggregator
    {
        public async Task<List<AggregatedData>> AggregateOrderBookAsync(
            string symbol,
            decimal price,
            List<(Dictionary<decimal, decimal> Bids, Dictionary<decimal, decimal> Asks)> orderBookData,
            int[] depthPercentages,
            bool cumulative = true)
        {
            if (!orderBookData.Any()) return new List<AggregatedData>();

            var results = new List<AggregatedData>();
            decimal previousBidRange = 0m;
            decimal previousAskRange = 0m;

            foreach (var depthPercentage in depthPercentages)
            {
                decimal priceRange = price * (depthPercentage / 100m);

                decimal bidVolume = 0m;
                decimal askVolume = 0m;

                foreach (var snapshot in orderBookData)
                {
                    var depthBids = snapshot.Bids.Where(b => b.Key >= price - priceRange);
                    bidVolume += depthBids.Sum(b => b.Value);

                    var depthAsks = snapshot.Asks.Where(a => a.Key <= price + priceRange);
                    askVolume += depthAsks.Sum(a => a.Value);
                }

                decimal amount = bidVolume + askVolume;
                decimal delta = (bidVolume + askVolume > 0) ? (bidVolume - askVolume) / (bidVolume + askVolume) * 100 : 0;

                var result = new AggregatedData
                {
                    Date = DateTime.UtcNow.ToString("yyyy-MM-dd"), // Лише дата
                    Ask = askVolume,
                    Bid = bidVolume,
                    Price = price,
                    Depth = depthPercentage
                };

                results.Add(result);

                previousBidRange = priceRange;
                previousAskRange = priceRange;
            }

            return results;
        }
    }
}
