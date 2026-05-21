using Core.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Inbound.Services
{
    internal class BackupOrderBookService (
        IOrderBookBackup orderBookBackup,
        ILogger _logger)
        : BackgroundService
    {
        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Backup OrderBook Service started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var timer = new PeriodicTimer(TimeSpan.FromMinutes(5));

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
