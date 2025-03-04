namespace Shared
{
    public class OrderBookAggregator
    {
        public async Task<List<AggregatedData>> AggregateMinuteDataAsync(
            string symbol,
            List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)> minuteData,
            int[] depthPercentages,
            bool cumulative = true)
        {
            if (!minuteData.Any()) return new List<AggregatedData>();

            // Беремо останній знімок за хвилину
            var snapshot = minuteData.OrderByDescending(x => x.Timestamp).First();

            // Обчислюємо середню ціну (midPrice між найкращими Bid і Ask)
            var bestBidPrice = snapshot.Bids.OrderByDescending(b => b.Price).FirstOrDefault().Price;
            var bestAskPrice = snapshot.Asks.OrderBy(a => a.Price).FirstOrDefault().Price;
            var midPrice = (bestBidPrice + bestAskPrice) / 2;

            var results = new List<AggregatedData>();
            decimal previousBidRange = 0m;
            decimal previousAskRange = 0m;

            foreach (var depthPercentage in depthPercentages)
            {
                // Діапазон цін для глибини
                decimal priceRange = midPrice * (depthPercentage / 100m);

                // Сукупний обсяг для Bids у межах глибини
                var depthBids = snapshot.Bids.Where(b => b.Price >= midPrice - priceRange);
                decimal bidVolume = depthBids.Sum(b => b.Quantity);

                // Сукупний обсяг для Asks у межах глибини
                var depthAsks = snapshot.Asks.Where(a => a.Price <= midPrice + priceRange);
                decimal askVolume = depthAsks.Sum(a => a.Quantity);

                if (!cumulative)
                {
                    // Для Noncumulative виключаємо попередні рівні
                    var previousBids = snapshot.Bids.Where(b => b.Price >= midPrice - previousBidRange);
                    var previousAsks = snapshot.Asks.Where(a => a.Price <= midPrice + previousAskRange);
                    bidVolume -= previousBids.Sum(b => b.Quantity);
                    askVolume -= previousAsks.Sum(a => a.Quantity);
                }

                // amount: Загальний обсяг заявок (сума bid і ask)
                decimal amount = bidVolume + askVolume;

                // delta: Дисбаланс у відсотках
                decimal delta = (bidVolume + askVolume > 0) ? (bidVolume - askVolume) / (bidVolume + askVolume) * 100 : 0;

                // a7: Обсяг на глибині 7%
                decimal a7PriceRange = midPrice * 0.07m;
                decimal a7BidVolume = snapshot.Bids.Where(b => b.Price >= midPrice - a7PriceRange).Sum(b => b.Quantity);
                decimal a7AskVolume = snapshot.Asks.Where(a => a.Price <= midPrice + a7PriceRange).Sum(a => a.Quantity);
                decimal a7 = a7BidVolume + a7AskVolume;

                var result = new AggregatedData
                {
                    Date = snapshot.Timestamp.ToString("yyyy-MM-dd HH:mm"), // Формат без секунд і мілісекунд
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
