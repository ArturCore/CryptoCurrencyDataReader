using Core.DTO;
using Core.Interfaces;
using Domain;
using Infrastructure.Binance.Interfaces;

namespace Infrastructure.Binance
{
    public class BinanceOrderBookSnapshotSource : IOrderBookSnapshotSource
    {
        private readonly IBinanceClient _binanceClient;
        private readonly IBinanceOrderBookSnapshotMapper _mapper;

        public BinanceOrderBookSnapshotSource(IBinanceClient binanceClient, IBinanceOrderBookSnapshotMapper mapper)
        {
            _binanceClient = binanceClient ?? throw new ArgumentNullException(nameof(binanceClient));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        public async Task<IResponse<OrderBookSnapshot>> GetSnapshotAsync(string symbol, int limit, CancellationToken cancellationToken)
        {
            var externalOrderBook = await _binanceClient.GetOrderBookAsync(symbol, limit, cancellationToken);
            var orderBookSnapshot = _mapper.MapToSnapshot(externalOrderBook);
            orderBookSnapshot.Symbol = symbol;
            return Response<OrderBookSnapshot>.Success(orderBookSnapshot);
        }
    }
}
