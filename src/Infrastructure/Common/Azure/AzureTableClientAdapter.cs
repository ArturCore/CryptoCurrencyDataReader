using Azure.Data.Tables;

namespace Infrastructure.Common.Azure
{
    public class AzureTableClientAdapter
    {
        private readonly TableServiceClient _tableServiceClient;
        private readonly Dictionary<string, TableClient> _tables;

        public AzureTableClientAdapter(TableServiceClient tableServiceClient) 
        { 
            _tableServiceClient = tableServiceClient;
            _tables = new Dictionary<string, TableClient>();
        }

        //public async Task<TableClient> GetOrCreateTableAsync(string tableName)
        //{
        //    if (_tables.ContainsKey(tableName))
        //    {
        //        return _tables[tableName];
        //    }

        //    var tableClient = _tableServiceClient.GetTableClient(tableName);
        //    try
        //    {
        //        await tableClient.CreateIfNotExistsAsync();
        //        _tables[tableName] = tableClient;
        //        Console.WriteLine($"Created table '{tableName}'.");
        //    }
        //    catch (Exception ex)
        //    {
        //        if (ex.Message.Contains("TableAlreadyExists"))
        //        {
        //            Console.WriteLine($"Table '{tableName}' already exists, skipping creation.");
        //            _tables[tableName] = tableClient;
        //        }
        //        else
        //        {
        //            Console.WriteLine($"Error creating table '{tableName}': {ex.Message}");
        //            throw;
        //        }
        //    }

        //    return tableClient;
        //}
    }
}
