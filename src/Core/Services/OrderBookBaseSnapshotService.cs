using Core.Configurations;
using Core.Interfaces;
using Domain;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace Core.Services
{
    internal class OrderBookBaseSnapshotService(
        IBaseOrderBookSource orderBookSource,
        IOptions<OrderBookOptions> options,
        IOrderBookStore orderBookStore,
        ILogger<OrderBookBaseSnapshotService> logger)
        : IOrderBookBaseSnapshot
    {      
        public async Task ApplySnapshotsAsync(CancellationToken cancellationToken)
        {
            foreach (string symbol in options.Value.Symbols)
            {
                cancellationToken.ThrowIfCancellationRequested();

                IResponse<OrderBookSnapshot> snapshot = await snapshotRetryPipeline.ExecuteAsync(
                    async token =>
                    {
                        return await orderBookSource.GetSnapshotAsync(
                            symbol,
                            options.Value.Limit,
                            token);
                    },
                    cancellationToken);

                if (!IsSnapshotRequestSucceed(snapshot))
                    continue;

                bool created = await orderBookStore.CreateWithSnapshotAsync(
                    snapshot.Data,
                    cancellationToken);

                if (!created)
                {
                    logger.LogWarning(
                        "Failed to apply snapshot to order book store for {Symbol}",
                        snapshot.Data.Symbol);

                    continue;
                }

                logger.LogInformation(
                    "Successfully applied snapshot to order book store for {Symbol}",
                    snapshot.Data.Symbol);
            }
        }

        private bool IsSnapshotRequestSucceed(IResponse<OrderBookSnapshot>? snapshotResult)
        {
            if (snapshotResult is null)
            {
                logger.LogWarning("Snapshot response is null.");
                return false;
            }

            if (!snapshotResult.IsSuccess)
            {
                logger.LogWarning(
                    "Failed to get snapshot. Error: {ErrorMessage}",
                    snapshotResult.ErrorMessage ?? "unknown");

                return false;
            }

            if (snapshotResult.Data is null)
            {
                logger.LogWarning("Snapshot response data is null.");
                return false;
            }

            if ((snapshotResult.Data.Asks?.Count ?? 0) == 0 &&
                (snapshotResult.Data.Bids?.Count ?? 0) == 0)
            {
                logger.LogWarning(
                    "Base order book is empty and cannot be applied to the order book store for {Symbol}",
                    snapshotResult.Data.Symbol);

                return false;
            }

            logger.LogInformation(
                "Successfully retrieved snapshot for {Symbol}",
                snapshotResult.Data.Symbol);

            return true;
        }

        private readonly ResiliencePipeline<IResponse<OrderBookSnapshot>> snapshotRetryPipeline =
            new ResiliencePipelineBuilder<IResponse<OrderBookSnapshot>>()
                .AddRetry(new RetryStrategyOptions<IResponse<OrderBookSnapshot>>
                {
                    MaxRetryAttempts = 3,
                    Delay = TimeSpan.FromMilliseconds(250),
                    BackoffType = DelayBackoffType.Exponential,
                    UseJitter = true,

                    ShouldHandle = new PredicateBuilder<IResponse<OrderBookSnapshot>>()
                        .Handle<Exception>(ex => ex is not OperationCanceledException)
                        .HandleResult(response =>
                            response is null ||
                            !response.IsSuccess ||
                            response.Data is null ||
                            ((response.Data.Asks?.Count ?? 0) == 0 &&
                             (response.Data.Bids?.Count ?? 0) == 0)),

                    OnRetry = args =>
                    {
                        logger.LogWarning(
                            "Retrying snapshot request. Attempt: {Attempt}. Delay: {Delay}. Reason: {Reason}",
                            args.AttemptNumber + 1,
                            args.RetryDelay,
                            args.Outcome.Exception?.Message ?? "invalid snapshot response");

                        return ValueTask.CompletedTask;
                    }
                })
                .Build();
    }
}