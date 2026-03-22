using Binance.Net.Clients;
using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;
using Infrastructure.Binance.Interfaces;
using Infrastructure.Binance.Mappers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructure.Binance
{
    public class BinanceRestClientAdapter : IBinanceClient
    {
        private readonly BinanceRestClient _client;
        private readonly BinanceOrderBookSnapshotMapper _mapper;

        public BinanceRestClientAdapter(BinanceRestClient client, BinanceOrderBookSnapshotMapper mapper)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        public async Task<ExternalOrderBookDto> GetOrderBookAsync(string symbol, int limit, CancellationToken cancellationToken)
        {
            var sdkResult = await _client.SpotApi.ExchangeData.GetOrderBookAsync(symbol, limit, cancellationToken);
            return _mapper.Map(sdkResult);
        }
    }
}
