using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Core.Interfaces
{
    public interface IBaseOrderBookSource
    {
        Task<IResponse<OrderBookSnapshot>> GetSnapshotAsync(
            string symbol,
            int limit,
            CancellationToken cancellationToken);
        Task<IResponse<decimal>> GetSymbolPrice(
            string symbol,
            CancellationToken cancellationToken);
    }
}
