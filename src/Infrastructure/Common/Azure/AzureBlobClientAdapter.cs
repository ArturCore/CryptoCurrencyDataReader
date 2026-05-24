using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Core.Interfaces;
using Infrastructure.Common.Configurations;
using Microsoft.Extensions.Options;

namespace Infrastructure.Common.Azure
{
    public class AzureBlobClientAdapter : IBackupClient
    {
        private readonly BlobContainerClient blobContainerClient;

        public AzureBlobClientAdapter(
            IOptions<AzureOptions> options)
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(options.Value.ConnectionString);
            blobContainerClient = blobServiceClient.GetBlobContainerClient(options.Value.BlobContainer);
        }

        public async Task UploadAsync(string fileName, Stream content, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(fileName)) throw new ArgumentException("blobName must be provided", nameof(fileName));
            if (content is null) throw new ArgumentNullException(nameof(content));

            await blobContainerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var blobClient = blobContainerClient.GetBlobClient(fileName);

            var headers = new BlobHttpHeaders();

            await blobClient.UploadAsync(
                content, 
                new BlobUploadOptions { HttpHeaders = headers, AccessTier = AccessTier.Cold }, 
                cancellationToken)
                    .ConfigureAwait(false);
        }
    }
}
