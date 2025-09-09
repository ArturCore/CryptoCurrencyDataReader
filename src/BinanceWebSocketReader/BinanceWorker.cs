using Azure;
using Binance.Net.Clients;
using Binance.Net.Interfaces;
using Binance.Net.Objects.Models.Spot;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shared;
using System.Collections.Immutable;
using System.Text;
using System.Threading.Channels;

namespace BinanceWebSocketReader
{
    public class BinanceWorker : BackgroundService
    {
        private readonly BinanceSocketClient _socketClient;
        private readonly BinanceRestClient _restClient;
        private readonly AzureDbService _azureDbService;
        private readonly OrderBookAggregator _aggregator;
        private readonly ILogger<BinanceWorker> _logger;
        private readonly Channel<IBinanceEventOrderBook> channel;

        private BookSnapshot _snapshot =
            new(ImmutableDictionary<string, (ImmutableDictionary<decimal, decimal>, ImmutableDictionary<decimal, decimal>)>.Empty);

        private int UpdateInterval;
        private IEnumerable<string> ExchangeSymbols;
        private int[] DepthPercentages;

        public BinanceWorker(IConfiguration configuration, ILogger<BinanceWorker> logger, Channel<IBinanceEventOrderBook> channel)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _socketClient = new BinanceSocketClient();
            _restClient = new BinanceRestClient();

            UpdateInterval = Int32.Parse(configuration["UpdateInterval"]);
            ExchangeSymbols = configuration["ExchangeSymbols"]
                .Split(",")
                .Select(x => x.Trim())
                .ToList();
            DepthPercentages = configuration["DepthPercentages"]
                .Split(",")
                .Select(x => int.Parse(x.Trim()))
                .ToArray();

            string? connectionString = configuration["AzureStorageConnectionString"];
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("AzureStorageConnectionString is not set in appsettings.json or environment variables.");
            }

            _azureDbService = new AzureDbService(connectionString);
            _aggregator = new OrderBookAggregator();
            this.channel = channel;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker started at {Time}", DateTime.UtcNow);

            var readerTask = Task.Run(() => ReadChannelAsync(stoppingToken), stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await SubscribeToOrderBookAsync(ExchangeSymbols, UpdateInterval, stoppingToken);

                    await GetOrderBookAsync(ExchangeSymbols, 5000);

                    await DelayToNextMinuteBoundaryAsync(stoppingToken);

                    using var timer = new PeriodicTimer(TimeSpan.FromMinutes(1));
                    while (await timer.WaitForNextTickAsync(stoppingToken))
                    {
                        var currentPrices = await GetCurrentPricesAsync(ExchangeSymbols, stoppingToken);

                        var orderbookSnapshot = CreateOrderbookSnapshot();

                        foreach (var kv in orderbookSnapshot)
                        {
                            string? symbol = kv.Key;
                            var bySymbol = kv.Value;
                            if (bySymbol.Count == 0) continue;

                            var priceObj = currentPrices.FirstOrDefault(x => x.Symbol == symbol);
                            if (priceObj == null)
                            {
                                _logger.LogWarning("No current price for {Symbol} at {Time}", symbol, DateTime.UtcNow);
                                continue;
                            }

                            try
                            {
                                var aggregated = await _aggregator.AggregateOrderBookAsync(
                                    symbol, priceObj.Price, bySymbol, DepthPercentages, cumulative: true);

                                var ts = DateTime.UtcNow.ToString("yyyyMMddHHmm");
                                await _azureDbService.SaveAggregatedDataAsync(symbol, ts, aggregated, stoppingToken);

                                //for local debug
                                //await SaveAggregatedDataToFileAsync(symbol, ts, aggregated, stoppingToken);
                            }
                            catch (Exception exAgg)
                            {
                                _logger.LogError(exAgg, "Error processing {Symbol}: {Message}", symbol, exAgg.Message);
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Critical error at {Time}: {Message}", DateTime.UtcNow, ex.Message);
                    await Task.Delay(10000, stoppingToken);
                }
            }

            await readerTask;
        }

        private Task SubscribeToOrderBookAsync(IEnumerable<string> symbols, int? updateInterval, CancellationToken cancellationToken)
        {
            int retryAttempts = 3;
            int retryDelayMs = 5000;

            for (int attempt = 1; attempt <= retryAttempts; attempt++)
            {
                try
                {
                    var result = _socketClient.SpotApi.ExchangeData.SubscribeToOrderBookUpdatesAsync(
                        symbols,
                        updateInterval,
                        (update) =>
                        {
                            channel.Writer.TryWrite(update.Data);                            
                        },
                        cancellationToken
                    );
                    return Task.CompletedTask;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Attempt {Attempt} to subscribe failed: {Message}", attempt, ex.Message);
                    if (attempt == retryAttempts)
                    {
                        return Task.FromException(new ApplicationException($"Attempt {attempt} to subscribe failed: {ex.Message}"));
                    }
                    Task.Delay(retryDelayMs, cancellationToken);
                }
            }
            return Task.FromException(new ApplicationException("Max retries of subscribe orderbook reached"));
        }

        public async Task GetOrderBookAsync(IEnumerable<string> exchangeSymbols, int limit)
        {
            var books = _snapshot.Books;

            foreach (string symbol in exchangeSymbols)
            {
                var orderbookSnapshot = await _socketClient.SpotApi.ExchangeData.GetOrderBookAsync(symbol, limit);

                if (!orderbookSnapshot.Success)
                {
                    _logger.LogWarning("Failed to get order book snapshot for {Symbol}: {Error}",
                        symbol, orderbookSnapshot.Error?.Message);
                    continue;
                }

                var bids = orderbookSnapshot.Data.Result.Bids
                    .ToImmutableDictionary(x => x.Price, x => x.Quantity);
                var asks = orderbookSnapshot.Data.Result.Asks
                    .ToImmutableDictionary(x => x.Price, x => x.Quantity);

                books = books.SetItem(symbol, (bids, asks));
            }

            Interlocked.Exchange(ref _snapshot, new BookSnapshot(books));
        }

        private async Task<IEnumerable<BinancePrice>> GetCurrentPricesAsync(IEnumerable<string> symbols, CancellationToken cancellationToken)
        {
            List<BinancePrice> prices = new();
            try
            {
                var result = await _restClient.SpotApi.ExchangeData.GetPricesAsync(symbols, cancellationToken);
                if (result.Success)
                {
                    prices = result.Data.ToList();
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

        private Dictionary<string, List<(Dictionary<decimal, decimal>, Dictionary<decimal, decimal>)>> CreateOrderbookSnapshot()
        {
            var snap = Volatile.Read(ref _snapshot);

            return snap.Books.ToDictionary(
                    kv => kv.Key,
                    kv => {
                        var (bids, asks) = kv.Value;
                        return new List<(Dictionary<decimal, decimal>, Dictionary<decimal, decimal>)>
                        {
                            (bids.ToDictionary(x => x.Key, x => x.Value),
                             asks.ToDictionary(x => x.Key, x => x.Value))
                        };
                    },
                    StringComparer.OrdinalIgnoreCase);
        }

        //local debug only
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

        private async Task ReadChannelAsync(CancellationToken cancellationToken)
        {
            var batch = new List<IBinanceEventOrderBook>(2048);

            while (await channel.Reader.WaitToReadAsync(cancellationToken))
            {
                batch.Clear();

                while (channel.Reader.TryRead(out var upd))
                    batch.Add(upd);

                if (batch.Count == 0)
                {
                    continue;
                }

                var grouped = batch.GroupBy(u => u.Symbol);

                var books = _snapshot.Books;

                foreach (var g in grouped)
                {
                    var symbol = g.Key;

                    var (bids, asks) = books.TryGetValue(symbol, out var pair)
                        ? pair
                        : (ImmutableDictionary<decimal, decimal>.Empty,
                           ImmutableDictionary<decimal, decimal>.Empty);

                    Dictionary<decimal, decimal>? tmpB = null;
                    Dictionary<decimal, decimal>? tmpA = null;

                    foreach (var u in g)
                    {
                        foreach (var b in u.Bids)
                        {
                            tmpB ??= new();
                            tmpB[b.Price] = b.Quantity;
                        }
                        foreach (var a in u.Asks)
                        {
                            tmpA ??= new();
                            tmpA[a.Price] = a.Quantity;
                        }
                    }

                    if (tmpB is not null)
                        foreach (var kv in tmpB)
                            bids = bids.SetItem(kv.Key, kv.Value);

                    if (tmpA is not null)
                        foreach (var kv in tmpA)
                            asks = asks.SetItem(kv.Key, kv.Value);

                    books = books.SetItem(symbol, (bids, asks));
                }

                Interlocked.Exchange(ref _snapshot, new BookSnapshot(books));
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Worker stopping at {Time}", DateTime.UtcNow);

            await base.StopAsync(cancellationToken);
        }

        private static async Task DelayToNextMinuteBoundaryAsync(CancellationToken cancellationToken)
        {
            var now = DateTimeOffset.UtcNow;
            var next = new DateTimeOffset(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, TimeSpan.Zero).AddMinutes(1);
            var delay = next - now;
            if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
            await Task.Delay(delay, cancellationToken);
        }
    }
}

public record BookSnapshot(
    ImmutableDictionary<string, (ImmutableDictionary<decimal, decimal> Bids,
                                 ImmutableDictionary<decimal, decimal> Asks)> Books);