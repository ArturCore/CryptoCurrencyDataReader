using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructure.SnapshotPublisher.Binance.Interfaces
{
    public interface IBinanceRestClientAdapter
    {
        Task<ExternalOrderBookDto> GetOrderBookAsync(string symbol, int limit, CancellationToken cancellationToken);
        Task<decimal> GetCurrentPrice(string symbol, CancellationToken cancellationToken);
    }
}
