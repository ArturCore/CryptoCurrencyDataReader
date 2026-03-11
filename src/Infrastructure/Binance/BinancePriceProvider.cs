using Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
