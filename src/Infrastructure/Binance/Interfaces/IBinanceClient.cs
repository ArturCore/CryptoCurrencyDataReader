using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructure.Binance.Interfaces
{
    public interface IBinanceClient
    {
        Task<ExternalOrderBookDto> GetOrderBookAsync(string symbol, int limit, CancellationToken cancellationToken);
    }
}
