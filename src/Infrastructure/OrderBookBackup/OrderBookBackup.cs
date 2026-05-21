using Core.Configurations;
using Core.Interfaces;
using Domain;
using Infrastructure.OrderBookBackup.Interfaces;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Infrastructure.OrderBookBackup
{
    public class OrderBookBackup(
        IOptions<OrderBookOptions> options,
        IOrderBookStore orderBookStore,
        IAzureBlobClientAdapter blobClientAdapter) 
        : IOrderBookBackup
    {
        //TODO: add cancellation
        //TODO: add logging
        //TODO: call Azure
        public async Task Execute(CancellationToken cancellationToken)
        {
            try
            {
                foreach (string symbol in options.Value.Symbols)
                {
                    OrderBook? orderBook = orderBookStore.GetRawOrderBook(symbol);
                    if (orderBook == null) continue;

                    await UploadOrderBookSnapshot(orderBook, "Binance");
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        private async Task UploadOrderBookSnapshot(OrderBook orderBook, string exchangeName)
        {
            MemoryStream stream = await CreateOrderBookStream(orderBook);

            string blobName = $"snapshots/exchange={exchangeName}/pair={orderBook.Symbol}/latest.json.gz";

            await blobClientAdapter.UploadAsync(blobName, stream);
        }

        private async Task<MemoryStream> CreateOrderBookStream(OrderBook orderBook)
        {
            var stream = new MemoryStream();

            await JsonSerializer.SerializeAsync(stream, orderBook);

            stream.Position = 0;

            return stream;
        }
    }
}
