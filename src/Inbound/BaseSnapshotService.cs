using Core.Interfaces;
using Inbound;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Invound
{
    internal class BaseSnapshotService : BackgroundService
    {
        /// at the start of the project:
        /// get snapshot
        /// apply snapshot
        /// 
        private readonly ILogger<BaseSnapshotService> logger;
        private readonly IOrderBookBaseSnapshot baseOrderBookSnapshot;
        private readonly SnapshotPublisherOptions options;

        public BaseSnapshotService(
            IOrderBookBaseSnapshot baseOrderBookSnapshot,
            IOptions<SnapshotPublisherOptions> options,
            ILogger<BaseSnapshotService> logger)
        {
            this.baseOrderBookSnapshot = baseOrderBookSnapshot ?? throw new ArgumentNullException(nameof(baseOrderBookSnapshot));
            this.options = options?.Value ?? new SnapshotPublisherOptions { Symbols = new List<string>(), Limit = 0 };
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        protected async override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            var symbols = options?.Symbols ?? Enumerable.Empty<string>();

            if (!symbols.Any())
            {
                logger.LogError("No symbols configured for snapshot publishing. Exiting ExecuteAsync.");
                return;
            }

            foreach (var symbol in symbols)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    logger.LogInformation("Cancellation requested before processing symbol {Symbol}. Exiting.", symbol);
                    break;
                }

                const int maxAttempts = 3;
                var attempt = 0;
                var success = false;

                while (attempt < maxAttempts && !success)
                {
                    attempt++;

                    try
                    {
                        var res = await baseOrderBookSnapshot.GetSnapshotAsync(symbol, options.Limit, cancellationToken);

                        if (cancellationToken.IsCancellationRequested)
                        {
                            logger.LogInformation("Cancellation requested after snapshot retrieval for {Symbol}. Aborting further attempts.", symbol);
                            break;
                        }

                        if (res is null)
                        {
                            logger.LogWarning("GetSnapshotAsync returned null response for {Symbol} (attempt {Attempt}/{MaxAttempts}).", symbol, attempt, maxAttempts);
                        }
                        else if (!res.IsSuccess)
                        {
                            logger.LogWarning("Failed to get snapshot for {Symbol} (attempt {Attempt}/{MaxAttempts}). Error: {ErrorMessage}",
                                symbol, attempt, maxAttempts, res.ErrorMessage ?? "unknown");
                        }
                        else
                        {
                            logger.LogInformation("Successfully retrieved snapshot for {Symbol} (attempt {Attempt}). Levels: bids={BidCount}, asks={AskCount}",
                                symbol, attempt,
                                res.Data?.Bids?.Count ?? 0,
                                res.Data?.Asks?.Count ?? 0);

                            success = true;
                            // TODO: apply snapshot
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        logger.LogInformation("Operation canceled while fetching snapshot for {Symbol}.", symbol);
                        throw;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Unexpected error while getting snapshot for {Symbol} (attempt {Attempt}/{MaxAttempts}).", symbol, attempt, maxAttempts);
                    }

                    if (!success && attempt < maxAttempts)
                    {
                        var backoffMs = (int)(Math.Pow(2, attempt - 1) * 250);
                        var jitter = new Random().Next(0, 150);
                        var delay = backoffMs + jitter;

                        logger.LogDebug("Waiting {Delay}ms before next attempt for {Symbol}.", delay, symbol);

                        try
                        {
                            await Task.Delay(delay, cancellationToken);
                        }
                        catch (OperationCanceledException)
                        {
                            logger.LogInformation("Delay canceled while waiting to retry snapshot for {Symbol}.", symbol);
                            throw;
                        }
                    }
                }

                if (!success)
                {
                    logger.LogError("Failed to obtain snapshot for {Symbol} after {MaxAttempts} attempts. Skipping symbol.", symbol, maxAttempts);
                }

                try
                {
                    await Task.Delay(50, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    logger.LogInformation("Cancellation requested while delaying between symbols.");
                    throw;
                }
            }

            logger.LogInformation("SnapshotPublisherService ExecuteAsync completed.");
        }
    }
}
