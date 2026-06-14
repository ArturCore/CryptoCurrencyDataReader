namespace Core.Configurations
{
    public class OrderBookOptions
    {
        public int Limit { get; set; }
        required public List<string> Symbols { get; set; }
        required public int UpdateInterval { get; set; }
        required public List<int> AggregationLevels { get; set; }
    }
}
