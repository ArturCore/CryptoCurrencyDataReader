using Core.Configurations;
using Core.Extentions;
using Inbound.Workers;
using Infrastructure.Common.Configurations;
using Infrastructure.Common.Extentions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

HostApplicationBuilder builder = new HostApplicationBuilder();

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

await Host.CreateDefaultBuilder(args)
    .ConfigureServices((services) =>
    {
        services.AddSingleton<IConfiguration>(configuration);
        //TODO: stop service if no symbols
        services.Configure<OrderBookOptions>(configuration.GetSection("OrderBookOptions"));
        services.Configure<AzureOptions>(configuration.GetSection("AzureOptions"));

        services.AddInfrastructure(configuration);
        services.AddCoreServices(configuration);

        // core runtime services
        services.AddHostedService<BaseSnapshotWorker>();
        services.AddHostedService<OrderBookUpdateWorker>();
        services.AddHostedService<BackupOrderBookWorker>();
        services.AddHostedService<AggregationOrderBookWorker>();
    })
    .Build()
    .RunAsync();