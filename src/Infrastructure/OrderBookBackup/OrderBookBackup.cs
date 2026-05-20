using Core.Configurations;
using Core.Interfaces;
using Domain;
using System.Text.Json;

namespace Infrastructure.OrderBookBackup
{
    public class OrderBookBackup(
        OrderBookOptions options,
        IOrderBookStore orderBookStore) 
        : IOrderBookBackup
    {
        public async Task Execute()
        {
            try
            {
                foreach (string symbol in options.Symbols)
                {
                    OrderBook? orderBook = orderBookStore.GetRawOrderBook(symbol);
                    if (orderBook == null) continue;

                    await UploadOrderBookSnapshot(orderBook);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        private async Task UploadOrderBookSnapshot(OrderBook orderBook)
        {
            MemoryStream stream = await CreateOrderBookStream(orderBook);

            //call Asure
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
