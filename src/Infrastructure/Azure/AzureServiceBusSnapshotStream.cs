using Core.Interfaces;
using Domain;

namespace Infrastructure.Azure
{
    internal class AzureServiceBusSnapshotStream : IOrderBookSnapshotStream
    {
        public Task StartAsync(Func<OrderBookSnapshot, Task> onSnapshot, CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}
