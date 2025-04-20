namespace Shared
{
    public class OrderBookAggregator
    {
        public async Task<List<AggregatedData>> AggregateMinuteDataAsync(
            string symbol,
            List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)> dailyData,
            int[] depthPercentages,
            bool cumulative = true)
        {
            if (!dailyData.Any()) return new List<AggregatedData>();

            // Беремо останній знімок для визначення midPrice
            var latestSnapshot = dailyData.OrderByDescending(x => x.Timestamp).First();
            var bestBidPrice = latestSnapshot.Bids.OrderByDescending(b => b.Price).FirstOrDefault().Price;
            var bestAskPrice = latestSnapshot.Asks.OrderBy(a => a.Price).FirstOrDefault().Price;
            var midPrice = (bestBidPrice + bestAskPrice) / 2;

            var results = new List<AggregatedData>();
            decimal previousBidRange = 0m;
            decimal previousAskRange = 0m;

            foreach (var depthPercentage in depthPercentages)
            {
                decimal priceRange = midPrice * (depthPercentage / 100m);

                // Сумуємо обсяги за весь період (24 години)
                decimal bidVolume = 0m;
                decimal askVolume = 0m;

                foreach (var snapshot in dailyData)
                {
                    var depthBids = snapshot.Bids.Where(b => b.Price >= midPrice - priceRange);
                    bidVolume += depthBids.Sum(b => b.Quantity);

                    var depthAsks = snapshot.Asks.Where(a => a.Price <= midPrice + priceRange);
                    askVolume += depthAsks.Sum(a => a.Quantity);
                }

                if (!cumulative)
                {
                    decimal previousBidVolume = 0m;
                    decimal previousAskVolume = 0m;
                    foreach (var snapshot in dailyData)
                    {
                        var previousBids = snapshot.Bids.Where(b => b.Price >= midPrice - previousBidRange);
                        previousBidVolume += previousBids.Sum(b => b.Quantity);

                        var previousAsks = snapshot.Asks.Where(a => a.Price <= midPrice + previousAskRange);
                        previousAskVolume += previousAsks.Sum(a => a.Quantity);
                    }
                    bidVolume -= previousBidVolume;
                    askVolume -= previousAskVolume;
                }

                decimal amount = bidVolume + askVolume;
                decimal delta = (bidVolume + askVolume > 0) ? (bidVolume - askVolume) / (bidVolume + askVolume) * 100 : 0;

                decimal a7PriceRange = midPrice * 0.07m;
                decimal a7BidVolume = 0m;
                decimal a7AskVolume = 0m;
                foreach (var snapshot in dailyData)
                {
                    a7BidVolume += snapshot.Bids.Where(b => b.Price >= midPrice - a7PriceRange).Sum(b => b.Quantity);
                    a7AskVolume += snapshot.Asks.Where(a => a.Price <= midPrice + a7PriceRange).Sum(a => a.Quantity);
                }
                decimal a7 = a7BidVolume + a7AskVolume;

                var result = new AggregatedData
                {
                    Date = DateTime.UtcNow.ToString("yyyy-MM-dd"), // Лише дата
                    Ask = askVolume,
                    Bid = bidVolume,
                    Price = midPrice,
                    Amount = amount,
                    Delta = delta,
                    A7 = a7,
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
