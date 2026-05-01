using Core.DTO;
using Core.Interfaces;
using Domain;
using Infrastructure.SnapshotPublisher.Binance.Interfaces;

namespace Infrastructure.SnapshotPublisher.Binance
{
    public class BinanceOrderBookSnapshot : IOrderBookBaseSnapshot
    {
        private readonly IBinanceRestClientAdapter _binanceClient;
        private readonly IOrderBookMapper _mapper;

        public BinanceOrderBookSnapshot(IBinanceRestClientAdapter binanceClient, IOrderBookMapper mapper)
        {
            _binanceClient = binanceClient ?? throw new ArgumentNullException(nameof(binanceClient));
            _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        }

        public async Task<IResponse<OrderBookSnapshot>> GetSnapshotAsync(string symbol, int limit, CancellationToken cancellationToken)
        {
            var externalOrderBook = await _binanceClient.GetOrderBookAsync(symbol, limit, cancellationToken);
            if (externalOrderBook is null)
            {
                return Response<OrderBookSnapshot>.Failure($"External order book for '{symbol}' is null.");
            }

            var orderBookSnapshot = _mapper.MapToSnapshot(externalOrderBook, symbol);
            return Response<OrderBookSnapshot>.Success(orderBookSnapshot);
        }
    }
}
