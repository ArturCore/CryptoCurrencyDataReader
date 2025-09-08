using Binance.Net.Interfaces;
using BinanceWebSocketReader;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Threading.Channels;

IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services.AddSingleton<IConfiguration>(configuration);

        services.AddHostedService<BinanceWorker>();

        services.AddSingleton(Channel.CreateUnbounded<IBinanceEventOrderBook>());
    });

using var host = builder.Build();
await host.RunAsync();