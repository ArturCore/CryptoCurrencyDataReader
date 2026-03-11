using Core.Interfaces;
using Domain;

namespace Infrastructure.Azure
{
    internal class AzureServiceBusSnapshotPublisher : IOrderBookSnapshotPublisher
    {
        public Task PublishAsync(OrderBookSnapshot snapshot, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}
