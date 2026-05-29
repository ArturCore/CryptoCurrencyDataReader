namespace Core.Interfaces
{
    public interface IOrderBookAggregation
    {
        Task Execute(CancellationToken cancellationToken);
    }
}
