using Azure.Data.Tables;
using Core.Interfaces;
using Domain;
using Infrastructure.AggregationSnapshot.Azure;
using Infrastructure.Common.Configurations;
using Microsoft.Extensions.Options;

namespace Infrastructure.Common.Azure
{
    public class AzureTableClientAdapter : IAggregatedOrderBookStorage
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

        public async Task SaveAggregatedDataAsync(string symbol, int depth, AggregatedOrderBookEvent aggregatedData, CancellationToken cancellationToken)
        {
            var tableClient = await GetOrCreateTableAsync();

            AggregatedOrderBookEntity orderBookEntity = new()
            {
                PartitionKey = symbol,
                RowKey = DateTime.UtcNow.ToString("yyyyMMddHHmm") + "_" + depth,
                Depth = depth,
                Price = aggregatedData.Price,
                AskVolume = aggregatedData.AskVolume,
                BidVolume = aggregatedData.BidVolume
            };

            await tableClient.UpsertEntityAsync(orderBookEntity, TableUpdateMode.Replace, cancellationToken);
        }

        private async Task<TableClient> GetOrCreateTableAsync()
        {
            string tableName = "AggregatedOrderBookDepths";

            var tableClient = tableServiceClient.GetTableClient(tableName);
            try
            {
                await tableClient.CreateIfNotExistsAsync();
                tables[tableName] = tableClient;
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("TableAlreadyExists"))
                {
                    Console.WriteLine($"Table '{tableName}' already exists, skipping creation.");
                    tables[tableName] = tableClient;
                }
                else
                {
                    Console.WriteLine($"Error creating table '{tableName}': {ex.Message}");
                    throw;
                }
            }

            return tableClient;
        }
    }
}
