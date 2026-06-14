using Core.Configurations;
using Core.Interfaces;
using Domain;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace Core.Services
{
    /// <summary>
    /// Step 4. Backup raw order book data
    /// For active currency pairs raw order book can contain hundreds of thousands levels of bids and asks
    /// In case if project restarts, we'll lose all accumulated order book
    /// In aggregated order book chart this situation will look like falling order book 5x or 10x down
    /// To fix this inconsistency we should backup local order book to cheap storage
    /// This data will be used at the start of the project as a base snapshot
    /// 
    /// This service designed to create snapshots with raw price-volume pairs from local order book
    /// and save this snapshot in external storage
    /// </summary>
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
