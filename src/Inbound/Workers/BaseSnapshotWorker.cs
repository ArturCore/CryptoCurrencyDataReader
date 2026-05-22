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
            _logger.LogInformation("{ServiceName} started", nameof(BaseSnapshotWorker));

            await bookBaseSnapshot.ApplySnapshotsAsync(cancellationToken);
        }
    }
}
