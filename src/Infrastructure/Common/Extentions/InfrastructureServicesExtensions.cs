using Binance.Net.Clients;
using Core.Interfaces;
using Core.Mappers;
using Infrastructure.Common.Azure;
using Infrastructure.Common.Binance;
using Infrastructure.Common.Configurations;
using Infrastructure.OrderBookBackup.Interfaces;
using Infrastructure.SnapshotPublisher.Binance;
using Infrastructure.SnapshotPublisher.Binance.Interfaces;
using Infrastructure.SnapshotPublisher.Binance.Mappers;
using Infrastructure.UpdateSnapshot.Binance;
using Infrastructure.UpdateSnapshot.Binance.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Infrastructure.Common.Extentions
{
    public static class InfrastructureServicesExtensions
    {
        public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton(sp => new BinanceSocketClient()); // default options
            services.AddSingleton(sp => new BinanceRestClient()); // default options

            services.AddTransient<IBinanceRestClientAdapter, BinanceRestClientAdapter>();
            services.AddTransient<BinanceOrderBookSnapshotMapper>();
            services.AddTransient<IOrderBookMapper, OrderBookMapper>();
            services.AddTransient<IBinanceOrderBookSnapshotMapper, BinanceOrderBookSnapshotMapperAdapter>();
            services.AddTransient<IBaseOrderBookSource, BinanceOrderBookSnapshot>();

            services.AddTransient<IBinanceSocketClientAdapter, BinanceSocketClientAdapter>();
            services.AddTransient<IOrderBookUpdatesSource, BinanceOrderBookUpdates>();

            services.AddTransient<IOrderBookBackup, OrderBookBackup.OrderBookBackup>();
            services.AddTransient<IAzureBlobClientAdapter, AzureBlobClientAdapter>();

            services.AddSingleton<IOrderBookStore, OrderBookStore>();

            services.Configure<AzureOptions>(configuration.GetSection("AzureOptions"));

            return services;
        }
    }
}
