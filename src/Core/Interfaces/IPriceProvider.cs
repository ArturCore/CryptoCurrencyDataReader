namespace Core.Interfaces
{
    public interface IPriceProvider
    {
        Task<decimal> GetCurrentPriceAsync(string symbol, CancellationToken ct);
    }
}
