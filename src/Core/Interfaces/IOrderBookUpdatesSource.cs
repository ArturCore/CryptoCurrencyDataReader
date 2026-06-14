using Core.DTO;

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
