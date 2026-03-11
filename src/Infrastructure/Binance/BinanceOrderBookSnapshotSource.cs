using Application.Interfaces;
using Core.Interfaces;
using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
