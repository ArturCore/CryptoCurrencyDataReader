using Core.Configurations;
using Inbound.Services;
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
        services.Configure<OrderBookOptions>(configuration.GetSection("OrderBookOptions"));

        services.AddInfrastructure(configuration);
        services.AddHostedService<BaseSnapshotService>();
        services.AddHostedService<OrderBookUpdateService>();
        services.AddHostedService<BackupSnapshotService>();
    })
    .Build()
    .RunAsync();