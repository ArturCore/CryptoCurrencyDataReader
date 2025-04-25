using Binance.Net.Clients;
using Microsoft.Extensions.Hosting;
using Shared;
using Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.Objects;
using System.Text;
using System.Text.Json;
using CryptoExchange.Net.Interfaces;
using Azure.Core;

namespace BinanceWebSocketReader
{
    public class BinanceWorker : BackgroundService
    {
        private readonly BinanceSocketClient _socketClient;
        private readonly BinanceRestClient _restClient;
        private readonly AzureDbService _azureDbService;
        private readonly OrderBookAggregator _aggregator;
        private readonly Dictionary<string, List<(Dictionary<decimal, decimal> Bids, Dictionary<decimal, decimal> Asks)>> _currentOrderBook; // Використовуємо _currentOrderBook
        private readonly object _lock = new object();
        private readonly ILogger<BinanceWorker> _logger;

        private int UpdateInterval;
        private string ExchangeSymbol;
        private bool Synchronisation = false;
        private long LastUpdateId;

        public BinanceWorker(IConfiguration configuration, ILogger<BinanceWorker> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _socketClient = new BinanceSocketClient();
            _restClient = new BinanceRestClient();
            _currentOrderBook = new Dictionary<string, List<(Dictionary<decimal, decimal>, Dictionary<decimal, decimal>)>>();

            string connectionString = configuration["AzureStorageConnectionString"];
            UpdateInterval = Int32.Parse(configuration["UpdateInterval"]);
            ExchangeSymbol = configuration["ExchangeSymbol"];
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("AzureStorageConnectionString is not set in appsettings.json or environment variables.");
            }

            _azureDbService = new AzureDbService(connectionString);
            _aggregator = new OrderBookAggregator();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker started at {Time}", DateTime.UtcNow);
            var depthPercentages = new[] { 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100 };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var orderBookSubscription = await SubscribeToOrderBookAsync(ExchangeSymbol, UpdateInterval, cts.Token);
                    if (!orderBookSubscription.Success)
                    {
                        _logger.LogWarning("Failed to subscribe to order book updates: {Error}", orderBookSubscription.Error?.Message);
                        await Task.Delay(5000, stoppingToken);
                        continue;
                    }

                    GetOrderBookAsync(ExchangeSymbol, 5000);

                    while (!stoppingToken.IsCancellationRequested)
                    {
                        //wait till next minute
                        var now = DateTime.Now;
                        var secondsToWait = 60 - now.Second;
                        await Task.Delay(secondsToWait * 1000);

                        var currentPrice = await GetCurrentPricesAsync(ExchangeSymbol, stoppingToken);

                        // Copy data for aggregation
                        var deepCopyOfOrderBook = DeepCopyDailyData(_currentOrderBook);

                        foreach (var symbol in deepCopyOfOrderBook.Keys.ToList())
                        {
                            var deepCopyOfOrderBookBySymbol = deepCopyOfOrderBook[symbol];
                            if (!deepCopyOfOrderBookBySymbol.Any()) continue;

                            try
                            {
                                var aggregatedData = await _aggregator.AggregateOrderBookAsync(symbol, currentPrice, deepCopyOfOrderBookBySymbol, depthPercentages, cumulative: true);
                                string timestamp = DateTime.UtcNow.ToString("yyyyMMddHHmm");
                                await _azureDbService.SaveAggregatedDataAsync(symbol, timestamp, aggregatedData, stoppingToken);
                                // for debugging only
                                //await SaveAggregatedDataToFileAsync(symbol, timestamp, aggregatedData, stoppingToken);
                                _logger.LogInformation("Saved aggregated daily data for {Symbol} at {Timestamp} with price {Price}", symbol, timestamp, currentPrice);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error processing {Symbol} at {Time}: {Message}", symbol, DateTime.UtcNow, ex.Message);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Critical error in worker loop at {Time}: {Message}", DateTime.UtcNow, ex.Message);
                    await Task.Delay(10000, stoppingToken);
                }
            }
        }

        private async Task<CallResult<UpdateSubscription>> SubscribeToOrderBookAsync(string symbol, int? updateInterval, CancellationToken cancellationToken)
        {
            int retryAttempts = 3;
            int retryDelayMs = 5000;

            for (int attempt = 1; attempt <= retryAttempts; attempt++)
            {
                try
                {
                    var result = await _socketClient.SpotApi.ExchangeData.SubscribeToOrderBookUpdatesAsync(
                        symbol,
                        updateInterval,
                        (update) =>
                        {
                            if (Synchronisation)
                            {
                                lock (_lock)
                                {
                                    string sym = update.Data.Symbol;

                                    var newBids = update.Data.Bids.ToDictionary(b => b.Price, b => b.Quantity);
                                    var newAsks = update.Data.Asks.ToDictionary(a => a.Price, a => a.Quantity);

                                    var (existingBids, existingAsks) = _currentOrderBook[sym].Last();

                                    foreach (var bid in newBids)
                                        existingBids[bid.Key] = bid.Value;

                                    foreach (var ask in newAsks)
                                        existingAsks[ask.Key] = ask.Value;

                                    _currentOrderBook[sym] = new List<(Dictionary<decimal, decimal>, Dictionary<decimal, decimal>)>
                                    {
                                        (existingBids, existingAsks)
                                    };
                                }
                            }
                            else
                            {
                                //for synchronisation only
                                LastUpdateId = update.Data.LastUpdateId;
                            }
                        },
                        cancellationToken
                    );
                    return result;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Attempt {Attempt} to subscribe failed: {Message}", attempt, ex.Message);
                    if (attempt == retryAttempts)
                    {
                        return new CallResult<UpdateSubscription>(null, $"Max retries reached after {retryAttempts} attempts: {ex.Message}");
                    }
                    await Task.Delay(retryDelayMs, cancellationToken);
                }
            }
            return new CallResult<UpdateSubscription>(null, "Max retries reached");
        }

        public async void GetOrderBookAsync(string exchangeSymbol, int limit)
        {
            var orderbookSnapshot = _socketClient.SpotApi.ExchangeData.GetOrderBookAsync(ExchangeSymbol, 5000);
            if (orderbookSnapshot.Result.Data.Result.LastUpdateId >= LastUpdateId)
            {
                var newBids = orderbookSnapshot.Result.Data.Result.Bids.ToDictionary(b => b.Price, b => b.Quantity);
                var newAsks = orderbookSnapshot.Result.Data.Result.Asks.ToDictionary(a => a.Price, a => a.Quantity);

                _currentOrderBook[exchangeSymbol] = new List<(Dictionary<decimal, decimal> Bids, Dictionary<decimal, decimal> Asks)>();
                _currentOrderBook[exchangeSymbol].Add((newBids, newAsks));

                Synchronisation = true;
            } 
            else
            {
                await Task.Delay(500);
                GetOrderBookAsync(exchangeSymbol, limit);
            }
        }

        private async Task<decimal> GetCurrentPricesAsync(string symbol, CancellationToken cancellationToken)
        {
            decimal price = 0;
            try
            {
                var result = await _restClient.SpotApi.ExchangeData.GetPriceAsync(symbol, cancellationToken);
                if (result.Success)
                {
                    price = result.Data.Price;
                }
                else
                {
                    _logger.LogWarning("Failed to get current prices: {Error}", result.Error?.Message);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error fetching current prices: {Message}", ex.Message);
            }
            return price;
        }

        private Dictionary<string, List<(Dictionary<decimal, decimal>, Dictionary<decimal, decimal>)>> DeepCopyDailyData(
            Dictionary<string, List<(Dictionary<decimal, decimal>, Dictionary<decimal, decimal>)>> original)
        {
            return original.ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value.Select(entry =>
                    (
                        new Dictionary<decimal, decimal>(entry.Item1), // deep copy of Bids
                        new Dictionary<decimal, decimal>(entry.Item2)  // deep copy of Asks
                    )
                ).ToList()
            );
        }

        private async Task SaveAggregatedDataToFileAsync(string symbol, string timestamp, List<AggregatedData> aggregatedData, CancellationToken cancellationToken)
        {
            try
            {
                string fileName = $"{symbol}_{timestamp}.txt";
                string filePath = Path.Combine("E:\\CryptoData_Test", fileName);

                StringBuilder content = new StringBuilder();
                content.AppendLine($"Symbol: {symbol}");
                content.AppendLine($"Timestamp: {timestamp}");
                content.AppendLine("Aggregated Data:");
                content.AppendLine("Depth | Bid | Ask | Price");

                foreach (var data in aggregatedData)
                {
                    content.AppendLine($"{data.Depth}% | {data.Bid:F8} | {data.Ask:F8} | {data.Price:F8}");
                }

                await File.WriteAllTextAsync(filePath, content.ToString(), cancellationToken);
                _logger.LogInformation("Successfully saved aggregated data to {FilePath}", filePath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to save aggregated data to file for {Symbol} at {Timestamp}: {Message}", symbol, timestamp, ex.Message);
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Worker stopping at {Time}", DateTime.UtcNow);

            await base.StopAsync(cancellationToken);
        }
    }
}