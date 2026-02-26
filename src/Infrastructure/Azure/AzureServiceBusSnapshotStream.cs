using Application.Interfaces;
using Domain;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
