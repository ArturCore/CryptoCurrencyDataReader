using Binance.Net.Clients;
using Core;
using Core.DTO;
using Core.Interfaces;
using Domain;
using Infrastructure.Binance.Mappers;

namespace Infrastructure.Binance
{
    public class BinanceOrderBookSnapshotSource : IOrderBookSnapshotSource
    {
        BinanceRestClient BinanceRestClient { get; set; }
        BinanceOrderBookSnapshotMapper Mapper { get; set; }

        BinanceOrderBookSnapshotSource(BinanceRestClient BinanceRestClient, BinanceOrderBookSnapshotMapper Mapper)
        {
            this.BinanceRestClient = BinanceRestClient ?? throw new ArgumentNullException();
            this.Mapper = Mapper ?? throw new ArgumentNullException();
        }

        public async Task<IResponse<OrderBookSnapshot>> GetSnapshotAsync(string symbol, int limit, CancellationToken cancellationToken)
        {
            var sdkResult = await BinanceRestClient.SpotApi.ExchangeData.GetOrderBookAsync(symbol, limit, cancellationToken);
            var externalOrderBook = Mapper.Map(sdkResult);

            OrderBookSnapshot orderBookSnapshot = Mapper.Map(externalOrderBook);
            orderBookSnapshot.Symbol = symbol;

            return Response<OrderBookSnapshot>.Success(orderBookSnapshot);
        }
    }
}
