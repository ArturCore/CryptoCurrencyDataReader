using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BinanceWebSocketReader
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            // Налаштування конфігурації
            IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var builder = Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    // Додаємо конфігурацію
                    services.AddSingleton<IConfiguration>(configuration);

                    // Додаємо основний сервіс WebJob
                    services.AddHostedService<BinanceWorker>();

                    // Логування версії для перевірки розгортання
                    Console.WriteLine("WebJob version: 2.0.0, deployed 2025-08-07");
                });

            using var host = builder.Build();
            await host.RunAsync();
        }
    }
}