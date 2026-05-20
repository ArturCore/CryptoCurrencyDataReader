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
            while (stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(20));

                await orderBookBackup.Execute();               
            }
        }
    }
}
