using Core.Interfaces;
using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
