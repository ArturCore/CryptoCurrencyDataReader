using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Infrastructure.OrderBookBackup.Interfaces
{
    public interface IAzureBlobClientAdapter
    {
        Task UploadAsync(
            string blobName,
            Stream content,
            string contentType = null,
            CancellationToken cancellationToken = default);
    }
}
