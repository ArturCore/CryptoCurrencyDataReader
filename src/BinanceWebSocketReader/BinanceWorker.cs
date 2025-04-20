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
        private readonly BinanceRestClient _restClient;
        private readonly AzureDbService _azureDbService;
        private readonly OrderBookAggregator _aggregator;
        private readonly Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>> _dailyData; // Використовуємо _dailyData
        private readonly Dictionary<string, decimal> _lastKnownPrices;
        private readonly object _lock = new object();
        private readonly ILogger<BinanceWorker> _logger;

        public BinanceWorker(IConfiguration configuration, ILogger<BinanceWorker> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _socketClient = new BinanceSocketClient();
            _restClient = new BinanceRestClient();
            _dailyData = new Dictionary<string, List<(DateTime, List<(decimal, decimal)>, List<(decimal, decimal)>)>>(); // Дані за 24 години
            _lastKnownPrices = new Dictionary<string, decimal>();

            string connectionString = configuration["AzureStorageConnectionString"];
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
            int? updateInterval = 100;
            var symbols = new[] { "ETHUSDT", "BTCUSDT" };
            var depthPercentages = new[] { 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100 };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var orderBookSubscription = await SubscribeToOrderBookAsync(symbols, updateInterval, cts.Token);
                    if (!orderBookSubscription.Success)
                    {
                        _logger.LogWarning("Failed to subscribe to order book updates: {Error}", orderBookSubscription.Error?.Message);
                        await Task.Delay(5000, stoppingToken);
                        continue;
                    }

                    //DateTime now = DateTime.UtcNow;
                    //DateTime nextMinute = now.AddMinutes(1).AddSeconds(-now.Second).AddMilliseconds(-now.Millisecond);
                    //int delayUntilNextMinute = (int)(nextMinute - now).TotalMilliseconds;

                    //_logger.LogInformation("Waiting until {NextMinute} (delay: {Delay} ms)", nextMinute, delayUntilNextMinute);
                    //await Task.Delay(delayUntilNextMinute, stoppingToken);

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

                        // Очищаємо дані старші за 24 години
                        lock (_lock)
                        {
                            foreach (var symbol in _dailyData.Keys.ToList())
                            {
                                _dailyData[symbol].RemoveAll(d => d.Timestamp < DateTime.UtcNow.AddDays(-1));
                            }
                        }

                        // Копіюємо дані для агрегації (глибока копія)
                        Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>> dataToAggregate;
                        lock (_lock)
                        {
                            dataToAggregate = new Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>>();
                            foreach (var kvp in _dailyData)
                            {
                                var copiedList = new List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>();
                                foreach (var entry in kvp.Value)
                                {
                                    var copiedBids = new List<(decimal Price, decimal Quantity)>(entry.Bids);
                                    var copiedAsks = new List<(decimal Price, decimal Quantity)>(entry.Asks);
                                    copiedList.Add((entry.Timestamp, copiedBids, copiedAsks));
                                }
                                dataToAggregate[kvp.Key] = copiedList;
                            }
                            _logger.LogInformation("Aggregating data for {Count} symbols over the last 24 hours", dataToAggregate.Count);
                        }

                        var currentPrices = await GetCurrentPricesAsync(symbols, stoppingToken);

                        foreach (var symbol in dataToAggregate.Keys.ToList())
                        {
                            var dailyData = dataToAggregate[symbol];
                            if (!dailyData.Any()) continue;

                            try
                            {
                                decimal currentPrice = currentPrices.ContainsKey(symbol) ? currentPrices[symbol] : 0;
                                decimal lastPrice = _lastKnownPrices.ContainsKey(symbol) ? _lastKnownPrices[symbol] : currentPrice;
                                decimal effectivePrice = currentPrice > 0
                                    ? (lastPrice > 0 ? (currentPrice + lastPrice) / 2 : currentPrice)
                                    : lastPrice;

                                //var correctedDailyData = CorrectOrderBookData(dailyData, effectivePrice);
                                var aggregatedData = await _aggregator.AggregateMinuteDataAsync(symbol, dailyData, depthPercentages, cumulative: true);
                                string timestamp = DateTime.UtcNow.ToString("yyyyMMddHHmm");
                                await _azureDbService.SaveAggregatedDataAsync(symbol, timestamp, aggregatedData, stoppingToken);
                                _logger.LogInformation("Saved aggregated daily data for {Symbol} at {Timestamp} with price {Price}", symbol, timestamp, effectivePrice);

                                if (currentPrice > 0)
                                {
                                    _lastKnownPrices[symbol] = currentPrice;
                                }
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

        private async Task<Dictionary<string, decimal>> GetCurrentPricesAsync(string[] symbols, CancellationToken cancellationToken)
        {
            var prices = new Dictionary<string, decimal>();
            try
            {
                var result = await _restClient.SpotApi.ExchangeData.GetPricesAsync(symbols, cancellationToken);
                if (result.Success)
                {
                    foreach (var price in result.Data)
                    {
                        prices[price.Symbol] = price.Price;
                    }
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
            return prices;
        }

        private List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>
            CorrectOrderBookData(
                List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)> dailyData,
                decimal effectivePrice)
        {
            if (effectivePrice <= 0) return dailyData;

            var correctedData = new List<(DateTime, List<(decimal, decimal)>, List<(decimal, decimal)>)>();

            foreach (var entry in dailyData)
            {
                var bids = entry.Bids;
                var asks = entry.Asks;

                if (!bids.Any() || bids.Max(b => b.Price) < effectivePrice * 0.5m)
                {
                    bids = new List<(decimal, decimal)> { (effectivePrice * 0.995m, 0.1m) };
                }
                if (!asks.Any() || asks.Min(a => a.Price) > effectivePrice * 1.5m)
                {
                    asks = new List<(decimal, decimal)> { (effectivePrice * 1.005m, 0.1m) };
                }

                correctedData.Add((entry.Timestamp, bids, asks));
            }

            return correctedData;
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
                                if (!_dailyData.ContainsKey(symbol))
                                {
                                    _dailyData[symbol] = new List<(DateTime, List<(decimal, decimal)>, List<(decimal, decimal)>)>();
                                }

                                _dailyData[symbol].Add((
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
                dataToAggregate = new Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>>(_dailyData); // Замінили _minuteData на _dailyData
                _dailyData.Clear();
            }

            foreach (var symbol in dataToAggregate.Keys.ToList())
            {
                var dailyData = dataToAggregate[symbol];
                if (dailyData.Any())
                {
                    try
                    {
                        var aggregatedData = await _aggregator.AggregateMinuteDataAsync(symbol, dailyData, new[] { 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100 });
                        string timestamp = dailyData.First().Timestamp.ToString("yyyyMMdd");
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