using Microsoft.Extensions.Hosting;

namespace Inbound
{
    internal class OrderBookUpdateService : BackgroundService
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
