using Core.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Inbound.Workers
{
    internal class BackupOrderBookWorker (
        IOrderBookBackup orderBookBackup,
        ILogger<BackupOrderBookWorker> _logger)
        : BackgroundService
    {
        private const int periodicExecutionBackupInMinutes = 5;

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Backup OrderBook Service started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var timer = new PeriodicTimer(TimeSpan.FromMinutes(periodicExecutionBackupInMinutes));

                    while (await timer.WaitForNextTickAsync(stoppingToken))
                    {
                        await orderBookBackup.Execute(stoppingToken);
                    }
                }
                catch (OperationCanceledException ex)
                {
                    _logger.LogInformation(ex, "Backup OrderBook Service cancelled");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Backup OrderBook Service iteration failed");
                }
            }
        }
    }
}
