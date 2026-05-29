namespace Core.Interfaces
{
    public interface IOrderBookBaseSnapshot
    {
        Task ApplySnapshotsAsync(CancellationToken cancellationToken);
    }
}
