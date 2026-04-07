using Binance.Net.Clients;
using Core.Interfaces;
using Core.Mappers;
using Infrastructure.Common.Binance;
using Infrastructure.SnapshotPublisher.Binance;
using Infrastructure.SnapshotPublisher.Binance.Interfaces;
using Infrastructure.SnapshotPublisher.Binance.Mappers;
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
        public static IServiceCollection AddInfrastructure(this IServiceCollection services)
        {
            services.AddSingleton<BinanceRestClient>();

            services.AddTransient<IBinanceSnapshotOrderBookClient, BinanceRestClientAdapter>();
            services.AddSingleton<BinanceOrderBookSnapshotMapper>();
            services.AddSingleton<IOrderBookMapper, OrderBookMapper>();
            services.AddTransient<IBinanceOrderBookSnapshotMapper, BinanceOrderBookSnapshotMapperAdapter>();
            services.AddTransient<IOrderBookBaseSnapshot, BinanceOrderBookSnapshot>();

            return services;
        }
    }
}
