using System;
using Binance.Net.Clients;
using Core.DTO;
using Infrastructure.Binance.Interfaces;

namespace Infrastructure.Binance
{
    public class BinanceRestClientAdapter : IBinanceClient
    {
        private readonly BinanceRestClient _client;
        private readonly IBinanceOrderBookSnapshotMapper _mapper;

        public BinanceRestClientAdapter(BinanceRestClient client, IBinanceOrderBookSnapshotMapper mapper)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        public async System.Threading.Tasks.Task<ExternalOrderBookDto> GetOrderBookAsync(string symbol, int limit, System.Threading.CancellationToken cancellationToken)
        {
            var sdkResult = await _client.SpotApi.ExchangeData.GetOrderBookAsync(symbol, limit, cancellationToken);
            return _mapper.MapSdkResult(sdkResult);
        }
    }
}
