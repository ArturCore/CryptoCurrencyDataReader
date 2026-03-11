using Core.Interfaces;

namespace Infrastructure.Binance
{
    internal class BinancePriceProvider : IPriceProvider
    {
        public Task<decimal> GetCurrentPriceAsync(string symbol, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}
