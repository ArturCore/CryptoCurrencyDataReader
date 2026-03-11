using Core.Interfaces;
using Domain;

namespace Infrastructure.Binance
{
    internal class BinanceOrderBookSnapshotSource : IOrderBookSnapshotSource
    {
        IResponse<Task<OrderBookSnapshot>> IOrderBookSnapshotSource.GetSnapshotAsync(string symbol, int limit, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
