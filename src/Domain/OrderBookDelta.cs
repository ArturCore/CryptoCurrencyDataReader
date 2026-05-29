namespace Domain
{
    public class OrderBookDelta
    {
        public string Symbol { get; set; }
        public long? FirstUpdateId { get; set; }
        public long LastUpdateId { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Bids { get; set; }
        public IReadOnlyCollection<OrderBookLevel> Asks { get; set; }
    }
}
