using Binance.Net.Clients;
using Microsoft.Extensions.Hosting;
using Shared;
using Azure;
using Microsoft.Extensions.Configuration;

namespace BinanceWebSocketReader
{
    public class BinanceWorker : BackgroundService
    {
        private readonly BinanceSocketClient _socketClient;
        private readonly AzureDbService _azureDbService;
        private readonly OrderBookAggregator _aggregator;
        private readonly Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>> _minuteData;
        private readonly object _lock = new object();

        public BinanceWorker(IConfiguration configuration)
        {
            _socketClient = new BinanceSocketClient();
            _minuteData = new Dictionary<string, List<(DateTime, List<(decimal, decimal)>, List<(decimal, decimal)>)>>();

            // Отримуємо connectionString із IConfiguration
            string connectionString = configuration["AzureStorageConnectionString"];
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("AzureStorageConnectionString is not set in appsettings.json or environment variables.");
            }

            // Ініціалізація AzureDbService
            _azureDbService = new AzureDbService(connectionString);

            // Ініціалізація OrderBookAggregator
            _aggregator = new Shared.OrderBookAggregator();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            int? updateInterval = 100; // Оновлення раз на секунду
            var symbols = new[] { "ETHUSDT", "BTCUSDT" };
            var depthPercentages = new[] { 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100 };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            // Обчислюємо затримку до початку наступної повної хвилини
            DateTime now = DateTime.UtcNow;
            DateTime nextMinute = now.AddMinutes(1).AddSeconds(-now.Second).AddMilliseconds(-now.Millisecond);
            int delayUntilNextMinute = (int)(nextMinute - now).TotalMilliseconds;

            Console.WriteLine($"Waiting until {nextMinute:yyyy-MM-dd HH:mm:ss} to start collecting data (delay: {delayUntilNextMinute} ms).");
            await Task.Delay(delayUntilNextMinute, cts.Token);

            // Підписка на оновлення ордербука
            var orderBookSubscription = _socketClient.SpotApi.ExchangeData.SubscribeToOrderBookUpdatesAsync(
                symbols,
                updateInterval,
                (update) =>
                {
                    lock (_lock)
                    {
                        var orderBook = update.Data;
                        string symbol = orderBook.Symbol;

                        if (!_minuteData.ContainsKey(symbol))
                        {
                            _minuteData[symbol] = new List<(DateTime, List<(decimal, decimal)>, List<(decimal, decimal)>)>();
                        }

                        _minuteData[symbol].Add((
                            DateTime.UtcNow,
                            orderBook.Bids.Select(b => (b.Price, b.Quantity)).ToList(),
                            orderBook.Asks.Select(a => (a.Price, a.Quantity)).ToList()
                        ));
                    }
                },
                cts.Token
            );

            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    // Чекаємо до наступної повної хвилини
                    DateTime nowAfterDelay = DateTime.UtcNow;
                    DateTime nextMinuteAfterDelay = nowAfterDelay.AddMinutes(1).AddSeconds(-nowAfterDelay.Second).AddMilliseconds(-nowAfterDelay.Millisecond);
                    int delayUntilNextMinuteAfter = (int)(nextMinuteAfterDelay - nowAfterDelay).TotalMilliseconds;

                    if (delayUntilNextMinuteAfter > 0)
                    {
                        await Task.Delay(delayUntilNextMinuteAfter, cts.Token);
                    }

                    // Створюємо копію даних для агрегації
                    Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>> dataToAggregate;
                    lock (_lock)
                    {
                        dataToAggregate = new Dictionary<string, List<(DateTime Timestamp, List<(decimal Price, decimal Quantity)> Bids, List<(decimal Price, decimal Quantity)> Asks)>>(_minuteData);
                        _minuteData.Clear(); // Очищаємо буфер після створення копії
                    }

                    // Агрегуємо дані за останню хвилину
                    foreach (var symbol in dataToAggregate.Keys.ToList())
                    {
                        var minuteData = dataToAggregate[symbol];
                        if (!minuteData.Any()) continue;

                        // Агрегуємо дані
                        var aggregatedData = await _aggregator.AggregateMinuteDataAsync(symbol, minuteData, depthPercentages, cumulative: true);

                        // Зберігаємо агреговані дані в Table Storage
                        string timestamp = minuteData.First().Timestamp.ToString("yyyyMMddHHmm");
                        await _azureDbService.SaveAggregatedDataAsync(symbol, timestamp, aggregatedData, cts.Token);
                    }

                    Console.WriteLine($"Completed aggregation for minute starting at {nextMinuteAfterDelay:yyyy-MM-dd HH:mm:ss}.");
                }
                catch (TaskCanceledException)
                {
                    Console.WriteLine("Program stopped by cancellation token.");
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error in minute aggregation: {ex.Message}");
                }
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            var depthPercentages = new[] { 1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100 };
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
                    var aggregatedData = await _aggregator.AggregateMinuteDataAsync(symbol, minuteData, depthPercentages);
                    string timestamp = minuteData.First().Timestamp.ToString("yyyyMMddHHmm");
                    await _azureDbService.SaveAggregatedDataAsync(symbol, timestamp, aggregatedData, cancellationToken);
                }
            }
            await base.StopAsync(cancellationToken);
        }
    }
}
