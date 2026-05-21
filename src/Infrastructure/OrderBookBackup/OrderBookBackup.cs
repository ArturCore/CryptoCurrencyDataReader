using Azure.Storage.Blobs.Models;
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
        //TODO: add logging
        //TODO: add error handling
        public async Task Execute(CancellationToken cancellationToken)
        {
            try
            {
                foreach (string symbol in options.Value.Symbols)
                {
                    OrderBookSnapshot? orderBook = orderBookStore.TryGetSnapshot(symbol);
                    if (orderBook == null) continue;

                    await UploadOrderBookSnapshot(orderBook, "Binance", cancellationToken);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        private async Task UploadOrderBookSnapshot(OrderBookSnapshot orderBook, string exchangeName, CancellationToken cancellationToken)
        {
            MemoryStream stream = await CreateOrderBookStream(orderBook);

            string blobName = $"snapshots/exchange={exchangeName}/pair={orderBook.Symbol}/latest.json";

            await blobClientAdapter.UploadAsync(blobName, stream, AccessTier.Cold, cancellationToken);
        }

        private async Task<MemoryStream> CreateOrderBookStream(OrderBookSnapshot orderBook)
        {
            var stream = new MemoryStream();

            await JsonSerializer.SerializeAsync(stream, orderBook);

            stream.Position = 0;

            return stream;
        }
    }
}
