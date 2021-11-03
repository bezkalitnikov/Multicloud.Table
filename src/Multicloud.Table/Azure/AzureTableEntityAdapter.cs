using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using TableEntity = Multicloud.Table.Abstractions.TableEntity;

namespace Multicloud.Table.Azure
{
    internal class AzureTableEntityAdapter<T> : ITableEntity where T : TableEntity
    {
        /// <summary>
        /// Gets or sets the entity's partition key
        /// </summary>
        public string PartitionKey
        {
            get => InnerObject.PartitionKey;
            set => InnerObject.PartitionKey = value;
        }

        /// <summary>
        /// Gets or sets the entity's row key.
        /// </summary>
        public string RowKey
        {
            get => InnerObject.RowKey;
            set => InnerObject.RowKey = value;
        }

        /// <summary>
        /// Gets or sets the entity's Timestamp.
        /// </summary>
        public DateTimeOffset Timestamp
        {
            get => InnerObject.Timestamp;
            set => InnerObject.Timestamp = value;
        }

        /// <summary>
        /// Gets or sets the entity's current ETag.
        /// Set this value to '*' in order to blindly overwrite an entity as part of an update operation.
        /// </summary>
        public string ETag
        {
            get => InnerObject.ETag;
            set => InnerObject.ETag = value;
        }

        /// <summary>
        /// Place holder for the original entity
        /// </summary>
        public T InnerObject { get; set; } 

        public AzureTableEntityAdapter()
        {
            // If you would like to work with objects that do not have a default Ctor you can use (T)Activator.CreateInstance(typeof(T));
            InnerObject = (T)Activator.CreateInstance(typeof(T), string.Empty, string.Empty);
        }

        public AzureTableEntityAdapter(T innerObject)
        {
            InnerObject = innerObject;
        } 

        public virtual void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            Microsoft.Azure.Cosmos.Table.TableEntity.ReadUserObject(InnerObject, properties, operationContext);
        }

        public virtual IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return Microsoft.Azure.Cosmos.Table.TableEntity.WriteUserObject(InnerObject, operationContext);
        } 
    }
}
