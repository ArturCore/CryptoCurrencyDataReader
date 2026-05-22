using Core.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Inbound.Workers
{
    internal class BaseSnapshotWorker(
        ILogger<BaseSnapshotWorker> _logger,
        IOrderBookBaseSnapshot bookBaseSnapshot)
        : BackgroundService
    {
        protected async override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            try
            {
                _logger.LogInformation("{ServiceName} started", nameof(BaseSnapshotWorker));

                await bookBaseSnapshot.ApplySnapshotsAsync(cancellationToken);
            }            
            catch (OperationCanceledException ex)
            {
                _logger.LogInformation("{ServiceName} cancelled", nameof(BaseSnapshotWorker));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{ServiceName} thrown an unexpected error", nameof(BaseSnapshotWorker));
            }
        }
    }
}
