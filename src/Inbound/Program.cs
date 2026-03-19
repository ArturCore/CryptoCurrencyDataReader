using Core.Interfaces;
using Inbound;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Infrastructure.Extentions;

HostApplicationBuilder builder = new HostApplicationBuilder();

builder.Services.AddScoped<IOrderBookMapper>();
builder.Services.AddScoped<IOrderBookSnapshotSource>();

builder.Services.AddInfrastructure();

await Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddHostedService<AppWorker>();
    })
    .Build()
    .RunAsync();