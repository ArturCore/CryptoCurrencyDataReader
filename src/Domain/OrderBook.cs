namespace Domain
{
    public class OrderBook
    {
        public string Symbol { get; }
        private readonly Dictionary<decimal, decimal> _bids = new();
        private readonly Dictionary<decimal, decimal> _asks = new();

        public OrderBook(string symbol)
        {
            Symbol = symbol ?? throw new ArgumentNullException(nameof(symbol));
        }

        public void ApplySnapshot(OrderBookSnapshot snapshot)
        {
            if (snapshot is null)
                throw new ArgumentNullException(nameof(snapshot));

            if (snapshot.Symbol != Symbol)
                throw new InvalidOperationException("Snapshot symbol mismatch.");

            _bids.Clear();
            _asks.Clear();

            foreach (var bid in snapshot.Bids)
            {
                _bids[bid.Price] = bid.Volume;
            }

            foreach (var ask in snapshot.Asks)
            {
                _asks[ask.Price] = ask.Volume;
            }
        }

        public void ApplyDelta(OrderBookDelta delta)
        {
            if (delta is null)
                throw new ArgumentNullException(nameof(delta));

            if (delta.Symbol != Symbol)
                throw new InvalidOperationException("Delta symbol mismatch.");

            if (delta.Bids != null)
            {
                foreach (var b in delta.Bids)
                {
                    if (b.Volume == 0m)
                        _bids.Remove(b.Price);
                    else
                        _bids[b.Price] = b.Volume;
                }
            }

            if (delta.Asks != null)
            {
                foreach (var a in delta.Asks)
                {
                    if (a.Volume == 0m)
                        _asks.Remove(a.Price);
                    else
                        _asks[a.Price] = a.Volume;
                }
            }
        }

        public OrderBookSnapshot ToSnapshot()
        {
            var bids = _bids
                .OrderByDescending(kv => kv.Key)
                .Select(kv => new OrderBookLevel { Price = kv.Key, Volume = kv.Value })
                .ToList();

            var asks = _asks
                .OrderBy(kv => kv.Key)
                .Select(kv => new OrderBookLevel { Price = kv.Key, Volume = kv.Value })
                .ToList();

            return new OrderBookSnapshot
            {
                Symbol = Symbol,
                Bids = bids,
                Asks = asks
            };
        }
    }
}
