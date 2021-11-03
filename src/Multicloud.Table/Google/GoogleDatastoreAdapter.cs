using Multicloud.Table.Abstractions;
using Google.Api.Gax.Grpc;
using Google.Cloud.Datastore.V1;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Multicloud.Table.Google
{
    [TableProvider(Provider = Providers.GoogleDatastore)]
    internal class GoogleDatastoreAdapter : ITableClient
    {
        private const string ProjectIdKey = "ProjectId";

        private readonly string _projectId;

        private readonly ILogger<GoogleDatastoreAdapter> _logger = new NullLogger<GoogleDatastoreAdapter>();

        public GoogleDatastoreAdapter(IReadOnlyDictionary<string, string> options, StorageTableOptions storageTableOptions, ILoggerFactory loggerFactory = null) 
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            if (!options.TryGetValue(ProjectIdKey, out var projectId))
            {
                throw new ArgumentException($"{ProjectIdKey} is required.");
            }

            _projectId = projectId;

            if (storageTableOptions == null)
            {
                throw new ArgumentNullException(nameof(storageTableOptions));
            }

            if (storageTableOptions.EnableLogging && loggerFactory != null)
            {
                _logger = loggerFactory.CreateLogger<GoogleDatastoreAdapter>();
            }
        }

        public IAsyncEnumerable<TEntity> GetEntitiesAsync<TEntity>(string tableName, string partitionKey, IEnumerable<string> columnsToProjection = null, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var db = DatastoreDb.Create(_projectId, tableName);
            var query = new Query(partitionKey);
            
            return GetEntities<TEntity>(db, query, columnsToProjection, cancellationToken);
        }

        public async Task<TEntity> GetEntityAsync<TEntity>(string tableName, string partitionKey, string rowKey, IEnumerable<string> columnsToProjection = null, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var columnsArr = columnsToProjection as string[] ?? columnsToProjection?.ToArray();

                var db = DatastoreDb.Create(_projectId, tableName);
                var key = db.CreateKeyFactory(partitionKey).CreateKey(rowKey);
                var entity = await db.LookupAsync(key, callSettings: CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false);
                var result = entity.Deserialize<TEntity>(_logger, columnsArr);

                return result;
        }

        public async Task InsertEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var db = DatastoreDb.Create(_projectId, tableName);
            var record = new Entity
            {
                Key = db.CreateKeyFactory(entity.PartitionKey).CreateKey(entity.RowKey)
            };
            entity.Timestamp = DateTime.UtcNow;
            record.Properties.Add(entity.Serialize(_logger));

            using (var transaction = await db.BeginTransactionAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false))
            {
                transaction.Insert(record);
                await transaction.CommitAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false);
            }
        }

        public async Task InsertEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var entitiesArr = entities as TEntity[] ?? entities.ToArray();
            var db = DatastoreDb.Create(_projectId, tableName);
            var records = entitiesArr.Select(x =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var record = new Entity
                {
                    Key = db.CreateKeyFactory(x.PartitionKey).CreateKey(x.RowKey)
                };
                x.Timestamp = DateTime.UtcNow;
                record.Properties.Add(x.Serialize(_logger));

                return record;
            });

            using (var transaction = await db.BeginTransactionAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false))
            {
                transaction.Insert(records);
                await transaction.CommitAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false);
            }
        }

        public async Task UpdateEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var db = DatastoreDb.Create(_projectId, tableName);
            var record = new Entity
            {
                Key = db.CreateKeyFactory(entity.PartitionKey).CreateKey(entity.RowKey)
            };
            entity.Timestamp = DateTime.UtcNow;
            record.Properties.Add(entity.Serialize(_logger));

            using (var transaction = await db.BeginTransactionAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false))
            {
                transaction.Update(record);
                await transaction.CommitAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false);
            }
        }

        public async Task UpdateEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var entitiesArr = entities as TEntity[] ?? entities.ToArray();
            var db = DatastoreDb.Create(_projectId, tableName);
            var records = entitiesArr.Select(x =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var record = new Entity
                {
                    Key = db.CreateKeyFactory(x.PartitionKey).CreateKey(x.RowKey)
                };
                x.Timestamp = DateTime.UtcNow;
                record.Properties.Add(x.Serialize(_logger));

                return record;
            });

            using (var transaction = await db.BeginTransactionAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false))
            {
                transaction.Update(records);
                await transaction.CommitAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false);
            }
        }

        public async Task UpsertEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var db = DatastoreDb.Create(_projectId, tableName);
            var record = new Entity
            {
                Key = db.CreateKeyFactory(entity.PartitionKey).CreateKey(entity.RowKey)
            };
            entity.Timestamp = DateTime.UtcNow;
            record.Properties.Add(entity.Serialize(_logger));

            using (var transaction = await db.BeginTransactionAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false))
            {
                transaction.Upsert(record);
                await transaction.CommitAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false);
            }
        }

        public async Task UpsertEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var entitiesArr = entities as TEntity[] ?? entities.ToArray();
            var db = DatastoreDb.Create(_projectId, tableName);
            var records = entitiesArr.Select(x =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var record = new Entity
                {
                    Key = db.CreateKeyFactory(x.PartitionKey).CreateKey(x.RowKey)
                };
                x.Timestamp = DateTime.UtcNow;
                record.Properties.Add(x.Serialize(_logger));

                return record;
            });

            using (var transaction = await db.BeginTransactionAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false))
            {
                transaction.Upsert(records);
                await transaction.CommitAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false);
            }
        }

        public async Task DeleteEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var db = DatastoreDb.Create(_projectId, tableName);
            var record = new Entity
            {
                Key = db.CreateKeyFactory(entity.PartitionKey).CreateKey(entity.RowKey)
            };
            entity.Timestamp = DateTime.UtcNow;
            record.Properties.Add(entity.Serialize(_logger));

            using (var transaction = await db.BeginTransactionAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false))
            {
                transaction.Delete(record);
                await transaction.CommitAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false);
            }
        }

        public async Task DeleteEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken)
            where TEntity : TableEntity
        {
            var entitiesArr = entities as TEntity[] ?? entities.ToArray();
            var db = DatastoreDb.Create(_projectId, tableName);
            var records = entitiesArr.Select(x =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                var record = new Entity
                {
                    Key = db.CreateKeyFactory(x.PartitionKey).CreateKey(x.RowKey)
                };
                x.Timestamp = DateTime.UtcNow;
                record.Properties.Add(x.Serialize(_logger));

                return record;
            });

            using (var transaction = await db.BeginTransactionAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false))
            {
                transaction.Delete(records);
                await transaction.CommitAsync(CallSettings.FromCancellationToken(cancellationToken)).ConfigureAwait(false);
            }
        }

        private async IAsyncEnumerable<TEntity> GetEntities<TEntity>(DatastoreDb db, Query query, IEnumerable<string> columnsToProjection = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
            where TEntity : TableEntity
        {
            var columnsArr = columnsToProjection as string[] ?? columnsToProjection?.ToArray();
            var lazyQuery = db.RunQueryLazilyAsync(query);

            await foreach (var item in lazyQuery.ConfigureAwait(false).WithCancellation(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                yield return item.Deserialize<TEntity>(_logger, columnsArr);
            }
        }
    }
}
