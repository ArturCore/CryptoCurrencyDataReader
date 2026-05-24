using Core.Configurations;
using Core.Interfaces;
using Domain;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Core.Services
{
    internal class OrderBookBackupService
    (
        IOptions<OrderBookOptions> options,
        IOrderBookStore orderBookStore,
        IBackupClient backupClient,
        ILogger<OrderBookBackupService> _logger)
        : IOrderBookBackup
    {
        public async Task Execute(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Order book backup started. Symbols count: {options.Value.Symbols.Count}");

            foreach (string symbol in options.Value.Symbols)
            {
                try
                {
                    OrderBookSnapshot? orderBook = orderBookStore.TryGetSnapshot(symbol);
                    if (orderBook == null) continue;

                    await UploadOrderBookSnapshot(orderBook, "Binance", cancellationToken);

                    _logger.LogInformation($"Order book backup for symbol {options.Value.Symbols.Count} succeed");
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Order book backup was cancelled");
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Order book backup failed for {symbol}");
                }
            }
        }

        private async Task UploadOrderBookSnapshot(OrderBookSnapshot orderBook, string exchangeName, CancellationToken cancellationToken)
        {
            MemoryStream stream = await CreateOrderBookStream(orderBook);

            string blobName = $"snapshots/exchange={exchangeName}/pair={orderBook.Symbol}/latest.json";

            await backupClient.UploadAsync(
                blobName,
                stream,
                cancellationToken);
        }

        private async Task<MemoryStream> CreateOrderBookStream(OrderBookSnapshot orderBook)
        {
            var stream = new MemoryStream();

            await JsonSerializer.SerializeAsync(
                stream,
                orderBook);

            stream.Position = 0;

            return stream;
        }
    }
}
