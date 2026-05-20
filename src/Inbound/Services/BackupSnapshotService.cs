using Core.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Inbound.Services
{
    internal class BackupSnapshotService (
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
