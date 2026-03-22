using Binance.Net.Clients;
using Core.Interfaces;
using Infrastructure.Binance;
using Infrastructure.Binance.Mappers;
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
            services.AddTransient<IOrderBookSnapshotSource, BinanceOrderBookSnapshotSource>();
            services.AddTransient<BinanceOrderBookSnapshotMapper>();

            services.AddSingleton<BinanceRestClient>();

            return services;
        }
    }
}
