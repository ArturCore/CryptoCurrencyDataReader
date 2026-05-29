using Core.Interfaces;
using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookBaseSnapshot
    {
        Task ApplySnapshotsAsync(CancellationToken cancellationToken);
    }
}
