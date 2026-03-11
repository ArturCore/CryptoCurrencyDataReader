using Core.Interfaces;
using Domain;

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
