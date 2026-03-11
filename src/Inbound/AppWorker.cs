using Microsoft.Extensions.Hosting;

namespace Inbound
{
    internal class AppWorker : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                Console.WriteLine("Tick");
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}
