using Inbound;
using Infrastructure.Extentions;
using Invound;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

HostApplicationBuilder builder = new HostApplicationBuilder();

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

await Host.CreateDefaultBuilder(args)
    .ConfigureServices((services) =>
    {
        services.AddSingleton<IConfiguration>(configuration);
        services.Configure<SnapshotPublisherOptions>(configuration.GetSection("SnapshotPublisherOptions"));

        services.AddInfrastructure();
        services.AddHostedService<SnapshotPublisherService>();
    })
    .Build()
    .RunAsync();