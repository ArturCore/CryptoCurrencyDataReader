using Binance.Net.Clients;
using Core;
using Core.DTO;
using Core.Interfaces;
using Domain;
using Infrastructure.Binance.Interfaces;
using Infrastructure.Binance.Mappers;

namespace Infrastructure.Binance
{
    public class BinanceOrderBookSnapshotSource : IOrderBookSnapshotSource
    {
        private readonly IBinanceClient _binanceClient;
        private readonly BinanceOrderBookSnapshotMapper _mapper;

        public BinanceOrderBookSnapshotSource(IBinanceClient binanceClient, BinanceOrderBookSnapshotMapper mapper)
        {
            _binanceClient = binanceClient ?? throw new ArgumentNullException(nameof(binanceClient));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        public async Task<IResponse<OrderBookSnapshot>> GetSnapshotAsync(string symbol, int limit, CancellationToken cancellationToken)
        {
            var externalOrderBook = await _binanceClient.GetOrderBookAsync(symbol, limit, cancellationToken);
            var orderBookSnapshot = _mapper.Map(externalOrderBook);
            orderBookSnapshot.Symbol = symbol;
            return Response<OrderBookSnapshot>.Success(orderBookSnapshot);
        }
    }
}
