using Azure.Data.Tables;
using Infrastructure.Configurations;
using Microsoft.Extensions.Options;

namespace Infrastructure.Common.Azure
{
    public class AzureTableClientAdapter
    {
        private readonly TableServiceClient tableServiceClient;
        private readonly Dictionary<string, TableClient> tables;
        private readonly AzureOptions options;

        public AzureTableClientAdapter(
            IOptions<AzureOptions> options)
        {
            this.options = options.Value;
            tableServiceClient = new TableServiceClient(options.Value.ConnectionString);
            tables = new Dictionary<string, TableClient>();
        }

        //public async Ta
        //sk<TableClient> GetOrCreateTableAsync(string tableName)
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
