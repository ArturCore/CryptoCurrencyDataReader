namespace Core.Interfaces
{
    public interface IBackupClient
    {
        Task UploadAsync(
            string fileName,
            Stream content,
            CancellationToken cancellationToken = default);
    }
}
