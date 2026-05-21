using Core.Interfaces;
using Microsoft.Extensions.Hosting;

namespace Inbound.Services
{
    internal class BackupOrderBookService (
        IOrderBookBackup orderBookBackup)
        : BackgroundService
    {
        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(20));

            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                await orderBookBackup.Execute(stoppingToken);
            }
        }
    }
}
