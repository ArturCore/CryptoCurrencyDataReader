using Domain;
using System.Runtime.CompilerServices;

namespace Core.Interfaces
{
    public interface IOrderBookUpdates
    {
        Task RunOrderBookUpdates(CancellationToken cancellationToken);
    }
}
