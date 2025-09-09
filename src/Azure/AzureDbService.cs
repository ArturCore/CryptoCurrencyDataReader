using Azure.Data.Tables;
using Shared;

namespace Azure
{
    public class AzureDbService
    {
        private readonly TableServiceClient _tableServiceClient;
        private readonly Dictionary<string, TableClient> _tables;

        public AzureDbService(string connectionString)
        {
            _tableServiceClient = new TableServiceClient(connectionString);
            _tables = new Dictionary<string, TableClient>();
        }

        private async Task<TableClient> GetOrCreateTableAsync(string tableName)
        {
            if (_tables.ContainsKey(tableName))
            {
                return _tables[tableName];
            }

            var tableClient = _tableServiceClient.GetTableClient(tableName);
            try
            {
                await tableClient.CreateIfNotExistsAsync();
                _tables[tableName] = tableClient;
                Console.WriteLine($"Created table '{tableName}'.");
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("TableAlreadyExists"))
                {
                    Console.WriteLine($"Table '{tableName}' already exists, skipping creation.");
                    _tables[tableName] = tableClient;
                }
                else
                {
                    Console.WriteLine($"Error creating table '{tableName}': {ex.Message}");
                    throw;
                }
            }

            return tableClient;
        }

        public async Task SaveAggregatedDataAsync(string symbol, string timestamp, List<AggregatedData> aggregatedData, CancellationToken cancellationToken)
        {
            var groupedData = aggregatedData.GroupBy(d => d.Depth);

            foreach (var group in groupedData)
            {
                int depth = group.Key;
                string tableName = $"Test{symbol}Depth{depth}";
                var tableClient = await GetOrCreateTableAsync(tableName);

                foreach (var data in group)
                {
                    var entity = new AggregatedOrderBookEntity(symbol, timestamp, data);
                    await tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken);
                }
            }
        }
    }
}
