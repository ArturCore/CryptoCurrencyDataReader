namespace Domain
{
    public class OrderBook
    {
        public string Symbol { get; }
        private readonly Dictionary<decimal, decimal> _bids = new();
        private readonly Dictionary<decimal, decimal> _asks = new();

        public OrderBook(string symbol)
        {
            Symbol = symbol;
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
            throw new NotImplementedException();
        }

        public OrderBookSnapshot ToSnapshot()
        {
            throw new NotImplementedException();
        }
    }
}
