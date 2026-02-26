using Application.Interfaces;
using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructure.Binance
{
    internal class BinanceOrderBookDeltaStream : IOrderBookDeltaStream
    {
        public Task StartAsync(string symbol, Func<OrderBookDelta, Task> onDelta, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
