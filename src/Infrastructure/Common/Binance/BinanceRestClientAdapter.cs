using Binance.Net.Clients;
using Binance.Net.Objects.Models.Spot;
using Core.DTO;
using CryptoExchange.Net.Objects;
using Infrastructure.SnapshotPublisher.Binance.Interfaces;

namespace Infrastructure.Common.Binance
{
    public class BinanceRestClientAdapter : IBinanceRestClientAdapter
    {
        private readonly BinanceRestClient _client;
        private readonly IBinanceOrderBookSnapshotMapper _mapper;

        public BinanceRestClientAdapter(BinanceRestClient client, IBinanceOrderBookSnapshotMapper mapper)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        public async Task<ExternalOrderBookDto> GetOrderBookAsync(string symbol, int limit, CancellationToken cancellationToken)
        {
            // TODO: error handling
            // FIX: mapping should be not here
            var sdkResult = await _client.SpotApi.ExchangeData.GetOrderBookAsync(symbol, limit, cancellationToken);
            return _mapper.MapSdkResult(sdkResult);
        }

        public async Task<decimal> GetCurrentPrice(string symbol, CancellationToken cancellationToken)
        {
            // TODO: error handling
            WebCallResult<BinancePrice> price = await _client.SpotApi.ExchangeData.GetPriceAsync(symbol, cancellationToken);
            return price.Data.Price;
        }
    }
}
