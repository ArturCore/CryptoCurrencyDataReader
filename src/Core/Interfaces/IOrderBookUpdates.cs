namespace Core.Interfaces
{
    public interface IOrderBookUpdates
    {
        Task RunOrderBookUpdates(CancellationToken cancellationToken);
    }
}
