using Infrastructure.OrderBookBackup;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Inbound.Workers
{
    internal class AggregationOrderBookWorker(
        ILogger<AggregationOrderBookWorker> _logger)
        : BackgroundService
    {
        protected async override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation($"{nameof(AggregationOrderBookWorker)} started");

            while (!cancellationToken.IsCancellationRequested)
            {
                Task.Delay(DelayUntilNextMinute(), cancellationToken);

                try
                {
                     //await orderBookBackup.Execute(cancellationToken);                    
                }
                catch (OperationCanceledException ex)
                {
                    _logger.LogInformation(ex, $"{nameof(AggregationOrderBookWorker)} cancelled");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"{nameof(AggregationOrderBookWorker)} iteration failed");
                }
            }
        }

        private TimeSpan DelayUntilNextMinute()
        {
            var now = DateTimeOffset.Now;

            var nextMinute = new DateTimeOffset(
                now.Year,
                now.Month,
                now.Day,
                now.Hour,
                now.Minute,
                0,
                now.Offset
            ).AddMinutes(1);

            return nextMinute - now;
        }
    }
}
