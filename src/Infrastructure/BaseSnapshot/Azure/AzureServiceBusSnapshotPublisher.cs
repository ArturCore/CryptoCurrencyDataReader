using Core.Interfaces;
using Domain;

namespace Infrastructure.SnapshotPublisher.Azure
{
    internal class AzureServiceBusSnapshotPublisher : IOrderBookSnapshotPublisher
    {
        public async Task PublishAsync(OrderBookSnapshot snapshot, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}
