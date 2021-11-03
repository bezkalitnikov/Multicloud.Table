using Multicloud.Table.Abstractions;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using TableEntity = Multicloud.Table.Abstractions.TableEntity;

namespace Multicloud.Table.Azure
{
    [TableProvider(Provider = Providers.AzureTableStorage)]
    internal class AzureTableStorageAdapter : ITableClient
    {
        private const string ConnectionStringKey = "ConnectionString";

        private readonly CloudStorageAccount _storageAccount;

        private readonly ILogger<AzureTableStorageAdapter> _logger = new NullLogger<AzureTableStorageAdapter>();

        public AzureTableStorageAdapter(IReadOnlyDictionary<string, string> options, StorageTableOptions storageTableOptions, ILoggerFactory loggerFactory = null)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            if (!options.TryGetValue(ConnectionStringKey, out var connectionString))
            {
                throw new ArgumentException($"{ConnectionStringKey} is required.");
            }

            _storageAccount = CloudStorageAccount.Parse(connectionString);

            if (storageTableOptions == null)
            {
                throw new ArgumentNullException(nameof(storageTableOptions));
            }

            if (storageTableOptions.EnableLogging && loggerFactory != null)
            {
                _logger = loggerFactory.CreateLogger<AzureTableStorageAdapter>();
            }
        }

        public IAsyncEnumerable<TEntity> GetEntitiesAsync<TEntity>(string tableName, string partitionKey, IEnumerable<string> columnsToProjection = null, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var columnsArr = columnsToProjection as string[] ?? columnsToProjection?.ToArray();
            var table = GetTable(tableName);
            var query = new TableQuery<AzureTableEntityAdapter<TEntity>>()
                .Where(TableQuery.GenerateFilterCondition(TableEntityCommonProperties.PartitionKey,
                    QueryComparisons.Equal, partitionKey))
                .Select(columnsArr);

            return GetEntities(table, query, cancellationToken);
        }

        public async Task<TEntity> GetEntityAsync<TEntity>(string tableName, string partitionKey, string rowKey, IEnumerable<string> columnsToProjection = null, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var columnsArr = columnsToProjection as string[] ?? columnsToProjection?.ToArray();
            var table = GetTable(tableName);
            var retrieveOperation = TableOperation.Retrieve<AzureTableEntityAdapter<TEntity>>(partitionKey, rowKey, columnsArr?.ToList());
            var result = await table.ExecuteAsync(retrieveOperation, cancellationToken).ConfigureAwait(false);

            return ((AzureTableEntityAdapter<TEntity>) result?.Result)?.InnerObject;
        }

        public async Task InsertEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var table = GetTable(tableName);
            entity.ETag ??= "*";
            var operation = TableOperation.Insert(new AzureTableEntityAdapter<TEntity>(entity));
            await table.ExecuteAsync(operation, cancellationToken).ConfigureAwait(false);
        }

        public async Task InsertEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var entitiesArr = entities as TEntity[] ?? entities.ToArray();
            var table = GetTable(tableName);
            var batch = new TableBatchOperation();

            foreach (var entity in entitiesArr)
            {
                cancellationToken.ThrowIfCancellationRequested();
                entity.ETag ??= "*";
                batch.Insert(new AzureTableEntityAdapter<TEntity>(entity));
            }

            await table.ExecuteBatchAsync(batch, cancellationToken);
        }

        public async Task UpdateEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var table = GetTable(tableName);
            entity.ETag ??= "*";
            var operation = TableOperation.Merge(new AzureTableEntityAdapter<TEntity>(entity));
            await table.ExecuteAsync(operation, cancellationToken);
        }

        public async Task UpdateEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var entitiesArr = entities as TEntity[] ?? entities.ToArray();
            var table = GetTable(tableName);
            var batch = new TableBatchOperation();

            foreach (var entity in entitiesArr)
            {
                cancellationToken.ThrowIfCancellationRequested();
                entity.ETag ??= "*";
                batch.Merge(new AzureTableEntityAdapter<TEntity>(entity));
            }

            await table.ExecuteBatchAsync(batch, cancellationToken);
        }

        public async Task UpsertEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var table = GetTable(tableName);
            var operation = TableOperation.InsertOrMerge(new AzureTableEntityAdapter<TEntity>(entity));
            await table.ExecuteAsync(operation, cancellationToken);
        }

        public async Task UpsertEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var entitiesArr = entities as TEntity[] ?? entities.ToArray();
            var table = GetTable(tableName);
            var batch = new TableBatchOperation();

            foreach (var entity in entitiesArr)
            {
                cancellationToken.ThrowIfCancellationRequested();
                batch.InsertOrMerge(new AzureTableEntityAdapter<TEntity>(entity));
            }

            await table.ExecuteBatchAsync(batch, cancellationToken);
        }

        public async Task DeleteEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var table = GetTable(tableName);
            entity.ETag ??= "*";
            var operation = TableOperation.Delete(new AzureTableEntityAdapter<TEntity>(entity));
            await table.ExecuteAsync(operation, cancellationToken);
        }

        public async Task DeleteEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var entitiesArr = entities as TEntity[] ?? entities.ToArray();
            var table = GetTable(tableName);
            var batch = new TableBatchOperation();

            foreach (var entity in entitiesArr)
            {
                cancellationToken.ThrowIfCancellationRequested();
                entity.ETag ??= "*";
                batch.Delete(new AzureTableEntityAdapter<TEntity>(entity));
            }

            await table.ExecuteBatchAsync(batch, cancellationToken);
        }

        private CloudTable GetTable(string tableName)
        {
            var tableClient = _storageAccount.CreateCloudTableClient();

            return tableClient.GetTableReference(tableName);
        }

        private async IAsyncEnumerable<TEntity> GetEntities<TEntity>(CloudTable table, TableQuery<AzureTableEntityAdapter<TEntity>> query, [EnumeratorCancellation] CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var continuationToken = default(TableContinuationToken);

            do
            {
                var result = await table
                    .ExecuteQuerySegmentedAsync(query, continuationToken, cancellationToken)
                    .ConfigureAwait(false);
                continuationToken = result.ContinuationToken;

                foreach (var entityAdapter in result.Results)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    yield return entityAdapter.InnerObject;
                }

            } while (continuationToken != null && !cancellationToken.IsCancellationRequested);

            cancellationToken.ThrowIfCancellationRequested();
        }
    }
}
