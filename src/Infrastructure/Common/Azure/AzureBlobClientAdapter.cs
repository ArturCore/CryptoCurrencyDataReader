using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Infrastructure.Common.Configurations;
using Infrastructure.OrderBookBackup.Interfaces;
using Microsoft.Extensions.Options;

namespace Infrastructure.Common.Azure
{
    public class AzureBlobClientAdapter : IAzureBlobClientAdapter
    {
        private readonly BlobContainerClient blobContainerClient;

        public AzureBlobClientAdapter(
            IOptions<AzureOptions> options)
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(options.Value.ConnectionString);
            blobContainerClient = blobServiceClient.GetBlobContainerClient(options.Value.BlobContainer);
        }

        public async Task UploadAsync(string blobName, Stream content, AccessTier? accessTier = null, string contentType = null, CancellationToken cancellationToken = default)
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

            await blobClient.UploadAsync(
                content, 
                new BlobUploadOptions { HttpHeaders = headers, AccessTier = accessTier }, 
                cancellationToken)
                    .ConfigureAwait(false);
        }
    }
}
