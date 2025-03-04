using Microsoft.Azure.Cosmos.Table;
using Shared;

namespace Azure
{
    public class AzureDbService
    {
        private readonly CloudTableClient _tableClient;
        private readonly Dictionary<string, CloudTable> _tables;

        public AzureDbService(string connectionString)
        {
            // Ініціалізація Table Storage
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            _tableClient = storageAccount.CreateCloudTableClient();
            _tables = new Dictionary<string, CloudTable>();
        }

        private async Task<CloudTable> GetOrCreateTableAsync(string tableName)
        {
            if (_tables.ContainsKey(tableName))
            {
                return _tables[tableName];
            }

            var table = _tableClient.GetTableReference(tableName);
            try
            {
                await table.CreateIfNotExistsAsync();
                _tables[tableName] = table;
                Console.WriteLine($"Created table '{tableName}'.");
            }
            catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == 409)
            {
                Console.WriteLine($"Table '{tableName}' already exists, skipping creation.");
                _tables[tableName] = table;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating table '{tableName}': {ex.Message}");
                throw;
            }

            return table;
        }

        public async Task SaveAggregatedDataAsync(string symbol, string timestamp, List<AggregatedData> aggregatedData, CancellationToken cancellationToken)
        {
            // Групуємо дані за глибиною
            var groupedData = aggregatedData.GroupBy(d => d.Depth);

            foreach (var group in groupedData)
            {
                int depth = group.Key;
                // Формуємо ім'я таблиці без недозволених символів
                string tableName = $"{symbol}Depth{depth}";
                var table = await GetOrCreateTableAsync(tableName);

                foreach (var data in group)
                {
                    var entity = new AggregatedOrderBookEntity(symbol, timestamp, data);
                    var insertOperation = TableOperation.Insert(entity);
                    await table.ExecuteAsync(insertOperation);
                }

                Console.WriteLine($"Saved aggregated minute data for {symbol} (Depth: {depth}%) to Table Storage: {tableName}");
            }
        }
    }
}
