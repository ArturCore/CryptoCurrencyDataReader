using Core.DTO;
using Core.Interfaces;
using Domain;
using Infrastructure.SnapshotPublisher.Binance.Interfaces;

namespace Infrastructure.SnapshotPublisher.Binance
{
    public class BinanceOrderBookSnapshot : IBaseOrderBookSource
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

            var orderBookSnapshot = _mapper.MapToSnapshot(externalOrderBook);
            return Response<OrderBookSnapshot>.Success(orderBookSnapshot);
        }

        public async Task<IResponse<decimal>> GetSymbolPrice(string symbol, CancellationToken cancellationToken)
        {
            try
            {
                decimal price = await _binanceClient.GetCurrentPrice(symbol, cancellationToken);

                return Response<decimal>.Success(price);
            }
            catch (Exception ex)
            {
                return Response<decimal>.Failure($"Unexpected error while trying to get symbol price");
            }
        }
    }
}
