using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Multicloud.Table.Abstractions
{
    public interface ITableClient
    {
        Task<TEntity> GetEntityAsync<TEntity>(string tableName, string partitionKey, string rowKey, IEnumerable<string> columnsToProjection = null, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;

        IAsyncEnumerable<TEntity> GetEntitiesAsync<TEntity>(string tableName, string partitionKey, IEnumerable<string> columnsToProjection = null, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;

        Task InsertEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;

        Task InsertEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;

        Task UpdateEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;

        Task UpdateEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;

        Task UpsertEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;

        Task UpsertEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;

        Task DeleteEntityAsync<TEntity>(string tableName, TEntity entity, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;

        Task DeleteEntitiesAsync<TEntity>(string tableName, IEnumerable<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : TableEntity;
    }
}
