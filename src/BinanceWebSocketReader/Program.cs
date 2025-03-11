using BinanceWebSocketReader;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

IConfiguration configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddEnvironmentVariables()
    .Build();

string? port = Environment.GetEnvironmentVariable("PORT") ?? "8080";

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services.AddSingleton<IConfiguration>(configuration);
        services.AddHostedService<BinanceWorker>();
        services.AddSingleton<IWebHost>(provider =>
        {
            return new WebHostBuilder()
                .UseKestrel()
                .UseUrls($"http://0.0.0.0:{port}")
                .Configure(app =>
                {
                    app.Run(async context =>
                    {
                        await context.Response.WriteAsync("Hello from Binance WebSocket Service!");
                    });
                })
                .Build();
        });
        services.AddHostedService<KestrelBackgroundService>();
    });

using var host = builder.Build();
await host.RunAsync();

/// <summary>
/// Фоновий сервіс для запуску Kestrel
/// </summary>
public class KestrelBackgroundService : BackgroundService
{
    private readonly IWebHost _webHost;

    public KestrelBackgroundService(IWebHost webHost)
    {
        _webHost = webHost;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _webHost.RunAsync(stoppingToken);
    }
}