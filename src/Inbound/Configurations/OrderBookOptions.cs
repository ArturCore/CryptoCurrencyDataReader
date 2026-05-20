namespace Inbound.Configurations
{
    internal class OrderBookOptions
    {
        public int Limit { get; set; }
        public List<string> Symbols { get; set; } = new List<string>();
    }
}
