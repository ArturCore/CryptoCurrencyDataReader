namespace Domain
{
    public class OrderBookDelta
    {
        public string Symbol { get; set; }
        public long UpdateId { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Bids { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Asks { get; set; }
    }
}
