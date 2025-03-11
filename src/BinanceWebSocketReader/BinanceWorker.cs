using Binance.Net.Clients;
using Microsoft.Extensions.Hosting;
using Shared;
using Azure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using CryptoExchange.Net.Objects.Sockets;
using CryptoExchange.Net.Objects;

namespace BinanceWebSocketReader
{
    public class BinanceWorker : BackgroundService
    {
        private readonly BinanceSocketClient _socketClient;
        private readonly AzureDbService _azureDbService;
        private readonly OrderBookAggregator _aggregator;
        private readonly Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>> _minuteData;
        private readonly object _lock = new object();
        private readonly ILogger<BinanceWorker> _logger;

        public BinanceWorker(IConfiguration configuration, ILogger<BinanceWorker> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _socketClient = new BinanceSocketClient();
            _minuteData = new Dictionary<string, List<(DateTime, List<(decimal, decimal)>, List<(decimal, decimal)>)>>();

            string connectionString = configuration["AzureStorageConnectionString"];
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("AzureStorageConnectionString is not set in appsettings.json or environment variables.");
            }

            _azureDbService = new AzureDbService(connectionString);
            _aggregator = new Shared.OrderBookAggregator();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker started at {Time}", DateTime.UtcNow);
            int? updateInterval = 100; // Оновлення раз на секунду
            var symbols = new[] { "ETHUSDT", "BTCUSDT" };
            var depthPercentages = new[] { 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100 };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            // Цикл для повторного підключення у випадку помилок
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Ініціалізація або повторне підключення до WebSocket
                    var orderBookSubscription = await SubscribeToOrderBookAsync(symbols, updateInterval, cts.Token);
                    if (!orderBookSubscription.Success)
                    {
                        _logger.LogWarning("Failed to subscribe to order book updates: {Error}", orderBookSubscription.Error?.Message);
                        await Task.Delay(5000, stoppingToken); // Пауза перед повторною спробою
                        continue;
                    }

                    // Очікування до початку хвилини
                    DateTime now = DateTime.UtcNow;
                    DateTime nextMinute = now.AddMinutes(1).AddSeconds(-now.Second).AddMilliseconds(-now.Millisecond);
                    int delayUntilNextMinute = (int)(nextMinute - now).TotalMilliseconds;

                    _logger.LogInformation("Waiting until {NextMinute} (delay: {Delay} ms)", nextMinute, delayUntilNextMinute);
                    await Task.Delay(delayUntilNextMinute, stoppingToken);

                    // Основний цикл агрегації
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        DateTime nowAfterDelay = DateTime.UtcNow;
                        DateTime nextMinuteAfterDelay = nowAfterDelay.AddMinutes(1).AddSeconds(-nowAfterDelay.Second).AddMilliseconds(-nowAfterDelay.Millisecond);
                        int delayUntilNextMinuteAfter = (int)(nextMinuteAfterDelay - nowAfterDelay).TotalMilliseconds;

                        _logger.LogInformation("Waiting until {NextMinute} (delay: {Delay} ms)", nextMinuteAfterDelay, delayUntilNextMinuteAfter);
                        if (delayUntilNextMinuteAfter > 0)
                        {
                            await Task.Delay(delayUntilNextMinuteAfter, stoppingToken);
                        }

                        Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>> dataToAggregate;
                        lock (_lock)
                        {
                            dataToAggregate = new Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>>(_minuteData);
                            _minuteData.Clear();
                            _logger.LogInformation("Aggregating data for {Count} symbols", dataToAggregate.Count);
                        }

                        foreach (var symbol in dataToAggregate.Keys.ToList())
                        {
                            var minuteData = dataToAggregate[symbol];
                            if (!minuteData.Any()) continue;

                            try
                            {
                                var aggregatedData = await _aggregator.AggregateMinuteDataAsync(symbol, minuteData, depthPercentages, cumulative: true);
                                string timestamp = minuteData.First().Timestamp.ToString("yyyyMMddHHmm");
                                await _azureDbService.SaveAggregatedDataAsync(symbol, timestamp, aggregatedData, stoppingToken);
                                _logger.LogInformation("Saved aggregated data for {Symbol} at {Timestamp}", symbol, timestamp);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error processing {Symbol} at {Time}: {Message}", symbol, DateTime.UtcNow, ex.Message);
                            }
                        }

                        _logger.LogInformation("Completed aggregation for minute starting at {Time}", nextMinuteAfterDelay);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Critical error in worker loop at {Time}: {Message}", DateTime.UtcNow, ex.Message);
                    await Task.Delay(10000, stoppingToken); // Пауза перед повторною спробою
                }
            }

            _logger.LogInformation("Worker stopping at {Time}", DateTime.UtcNow);
        }

        private async Task<CallResult<UpdateSubscription>> SubscribeToOrderBookAsync(string[] symbols, int? updateInterval, CancellationToken cancellationToken)
        {
            int retryAttempts = 3;
            int retryDelayMs = 5000;

            for (int attempt = 1; attempt <= retryAttempts; attempt++)
            {
                try
                {
                    var result = await _socketClient.SpotApi.ExchangeData.SubscribeToOrderBookUpdatesAsync(
                        symbols,
                        updateInterval,
                        (update) =>
                        {
                            lock (_lock)
                            {
                                _logger.LogDebug("Received update for {Symbol} at {Time}", update.Data.Symbol, DateTime.UtcNow);
                                string symbol = update.Data.Symbol;
                                if (!_minuteData.ContainsKey(symbol))
                                {
                                    _minuteData[symbol] = new List<(DateTime, List<(decimal, decimal)>, List<(decimal, decimal)>)>();
                                }

                                _minuteData[symbol].Add((
                                    DateTime.UtcNow,
                                    update.Data.Bids.Select(b => (b.Price, b.Quantity)).ToList(),
                                    update.Data.Asks.Select(a => (a.Price, a.Quantity)).ToList()
                                ));
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

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Worker stopping at {Time}", DateTime.UtcNow);

            Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>> dataToAggregate;
            lock (_lock)
            {
                dataToAggregate = new Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>>(_minuteData);
                _minuteData.Clear();
            }

            foreach (var symbol in dataToAggregate.Keys.ToList())
            {
                var minuteData = dataToAggregate[symbol];
                if (minuteData.Any())
                {
                    try
                    {
                        var aggregatedData = await _aggregator.AggregateMinuteDataAsync(symbol, minuteData, new[] { 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100 });
                        string timestamp = minuteData.First().Timestamp.ToString("yyyyMMddHHmm");
                        await _azureDbService.SaveAggregatedDataAsync(symbol, timestamp, aggregatedData, cancellationToken);
                        _logger.LogInformation("Saved remaining data for {Symbol} at {Timestamp}", symbol, timestamp);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error saving remaining data for {Symbol} at {Time}: {Message}", symbol, DateTime.UtcNow, ex.Message);
                    }
                }
            }

            await base.StopAsync(cancellationToken);
        }
    }
}