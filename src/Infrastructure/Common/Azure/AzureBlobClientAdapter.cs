using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Infrastructure.Configurations;

namespace Infrastructure.Common.Azure
{
    public class AzureBlobClientAdapter
    {
        private readonly BlobContainerClient blobContainerClient;
        private readonly AzureOptions options;

        public AzureBlobClientAdapter(
            AzureOptions options)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));

            BlobServiceClient blobServiceClient = new BlobServiceClient(options.ConnectionString);
            blobContainerClient = blobServiceClient.GetBlobContainerClient(options.BlobContainer);
        }

        public async Task UploadAsync(string blobName, Stream content, string contentType = null, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(blobName)) throw new ArgumentException("blobName must be provided", nameof(blobName));
            if (content is null) throw new ArgumentNullException(nameof(content));

            await blobContainerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var blobClient = blobContainerClient.GetBlobClient(blobName);

            var headers = new BlobHttpHeaders();
            if (!string.IsNullOrWhiteSpace(contentType))
            {
                headers.ContentType = contentType;
            }

            await blobClient.UploadAsync(content, new BlobUploadOptions { HttpHeaders = headers }, cancellationToken).ConfigureAwait(false);
        }
    }
}
