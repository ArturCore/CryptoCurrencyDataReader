using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
