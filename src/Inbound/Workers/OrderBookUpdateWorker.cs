using Core.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Inbound.Workers
{
    internal class OrderBookUpdateWorker(
        ILogger<OrderBookUpdateWorker> _logger,
        IOrderBookUpdates orderBookUpdates)
        : BackgroundService
    {
        protected async override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            try
            {
                _logger.LogInformation("{ServiceName} started", nameof(OrderBookUpdateWorker));

                await orderBookUpdates.RunOrderBookUpdates(cancellationToken);
            }
            catch (OperationCanceledException ex)
            {
                _logger.LogInformation("{ServiceName} cancelled", nameof(OrderBookUpdateWorker));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{ServiceName} thrown an unexpected error", nameof(OrderBookUpdateWorker));
            }    
        }
    }
}
