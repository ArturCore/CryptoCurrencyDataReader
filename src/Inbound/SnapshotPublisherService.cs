using Core.Interfaces;
using Inbound;
using Infrastructure.Binance;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Invound
{
    internal class SnapshotPublisherService : BackgroundService
    {
        /// at the start of the project:
        /// get snapshot
        /// apply snapshot
        /// 
        private readonly IOrderBookSnapshotSource orderBookSnapshotSource;
        private readonly SnapshotPublisherOptions options;

        public SnapshotPublisherService(
            IOrderBookSnapshotSource orderBookSnapshotSource,
            IOptions<SnapshotPublisherOptions> options)
        {
            this.orderBookSnapshotSource = orderBookSnapshotSource;
            this.options = options.Value;
        }

        protected async override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            foreach (var symbol in options.Symbols)
            {
                var res = await orderBookSnapshotSource.GetSnapshotAsync(symbol, options.Limit, cancellationToken);
            }

        }
    }
}
