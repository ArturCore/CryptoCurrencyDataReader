using Core.DTO;
using Domain;

namespace Core.Interfaces
{
    public interface IOrderBookUpdatesSource
    {
        Task SubscriteToStreamOrderBookUpdatesAsync(
            IReadOnlyCollection<string> symbols,
            int updateInterval,
            Func<ExternalOrderBookEventDto, CancellationToken, Task> onUpdateAsync,
            CancellationToken cancellationToken);
    }
}
