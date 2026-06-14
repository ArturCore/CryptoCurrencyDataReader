namespace Core.Interfaces
{
    public interface IOrderBookBackup
    {
        Task Execute(CancellationToken cancellationToken);
    }
}
