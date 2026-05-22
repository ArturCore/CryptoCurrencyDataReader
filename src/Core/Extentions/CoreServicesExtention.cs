using Core.Interfaces;
using Core.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Core.Extentions
{
    public static class CoreServicesExtention
    {
        public static IServiceCollection AddCoreServices(this IServiceCollection services, IConfiguration configuration)
        {
            services.AddTransient<IOrderBookBaseSnapshot, OrderBookBaseSnapshotService>();
            services.AddTransient<IOrderBookUpdates, OrderBookUpdatesService>();

            return services;
        }
    }
}
