using Binance.Net.Clients;
using Core.Interfaces;
using Core.Mappers;
using CryptoExchange.Net.SharedApis;
using Infrastructure.Common.Binance;
using Infrastructure.Configurations;
using Infrastructure.SnapshotPublisher.Binance;
using Infrastructure.SnapshotPublisher.Binance.Interfaces;
using Infrastructure.SnapshotPublisher.Binance.Mappers;
using Infrastructure.UpdateSnapshot.Binance;
using Infrastructure.UpdateSnapshot.Binance.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructure.Extentions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddSingleton(sp => new BinanceSocketClient()); // default options
            services.AddSingleton(sp => new BinanceRestClient()); // default options

            services.AddTransient<IBinanceRestClientAdapter, BinanceRestClientAdapter>();
            services.AddTransient<BinanceOrderBookSnapshotMapper>();
            services.AddTransient<IOrderBookMapper, OrderBookMapper>();
            services.AddTransient<IBinanceOrderBookSnapshotMapper, BinanceOrderBookSnapshotMapperAdapter>();
            services.AddTransient<IOrderBookBaseSnapshot, BinanceOrderBookSnapshot>();


            services.AddTransient<IBinanceSocketClientAdapter, BinanceSocketClientAdapter>();
            services.AddTransient<IOrderBookUpdates, BinanceOrderBookUpdates>();

            services.AddSingleton<IOrderBookStore, OrderBookStore>();

            services.Configure<AzureOptions>(configuration.GetSection("AzureOptions"));

            return services;
        }
    }
}
