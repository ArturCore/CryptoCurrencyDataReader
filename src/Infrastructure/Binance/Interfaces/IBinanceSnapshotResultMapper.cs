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
    internal interface IBinanceSnapshotResultMapper
    {
        ExternalOrderBookDto Map(WebCallResult<BinanceOrderBook> sdkResult);
    }
}
