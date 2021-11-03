using Multicloud.Table.Abstractions;
using Google.Cloud.Datastore.V1;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Multicloud.Table.Google
{
    internal static class GoogleEntityExtensions
    {
        // Indexed property in Google Datastore has limitation
        // 1500 byte in size
        // https://cloud.google.com/datastore/docs/concepts/entities
        private const int IndexedPropertyLimit = 1500;

        public static IDictionary<string, Value> Serialize<TEntity>(this TEntity entity, ILogger logger) where TEntity : TableEntity
        {
            if (entity == null)
            {
                return null;
            }

            var dictionary = new Dictionary<string, Value>();

            foreach (var property in entity.GetType().GetProperties())
            {
                if (!ShouldSkipProperty(property, logger))
                {
                    var propertyFromObject = CreatePropertyFromObject(property.GetValue(entity, null), property.PropertyType);

                    if (propertyFromObject != null)
                    {
                        dictionary.Add(property.Name, propertyFromObject);
                    }
                }
            }

            return dictionary;
        }

        public static TEntity Deserialize<TEntity>(this Entity entity, ILogger logger, IEnumerable<string> columnsToProjection = null) where TEntity : TableEntity
        {
            if (entity == null)
            {
                return null;
            }

            var properties = entity.Properties;
            var deserializedEntity = (TEntity)Activator.CreateInstance(typeof(TEntity), string.Empty, string.Empty);
            var columns = columnsToProjection as string[] ?? columnsToProjection?.ToArray();

            foreach (var propInfo in deserializedEntity.GetType().GetProperties())
            {
                if (!ShouldSkipProperty(propInfo, logger, columns))
                {
                    if (!properties.ContainsKey(propInfo.Name))
                    {
                        logger.LogInformation(
                            "Omitting property '{0}' from de-serialization because there is no corresponding entry in the dictionary provided.",
                            propInfo.Name);
                    }
                    else
                    {
                        var serializedProp = properties[propInfo.Name];

                        if (serializedProp.IsNull)
                        {
                            propInfo.SetValue(deserializedEntity, null, null);
                        }
                        else
                        {
                            switch (serializedProp.ValueTypeCase)
                            {
                                case Value.ValueTypeOneofCase.StringValue:
                                    if (propInfo.PropertyType == typeof(string))
                                    {
                                        propInfo.SetValue(deserializedEntity, serializedProp.StringValue, null);
                                    }
                                    else if (propInfo.PropertyType == typeof(Guid))
                                    {
                                        if (Guid.TryParse(serializedProp.StringValue, out var id))
                                        {
                                            propInfo.SetValue(deserializedEntity, id, null);
                                        }
                                    }

                                    continue;
                                case Value.ValueTypeOneofCase.BlobValue:
                                    if (propInfo.PropertyType == typeof(byte[]))
                                    {
                                        propInfo.SetValue(deserializedEntity, serializedProp.BlobValue.ToByteArray(), null);
                                    }

                                    continue;
                                case Value.ValueTypeOneofCase.BooleanValue:
                                    if (propInfo.PropertyType == typeof(bool) || propInfo.PropertyType == typeof(bool?))
                                    {
                                        propInfo.SetValue(deserializedEntity, serializedProp.BooleanValue, null);
                                    }

                                    continue;
                                case Value.ValueTypeOneofCase.TimestampValue:
                                    if (propInfo.PropertyType == typeof(DateTime) ||
                                        propInfo.PropertyType == typeof(DateTime?))
                                    {
                                        propInfo.SetValue(deserializedEntity, serializedProp.TimestampValue.ToDateTime(), null);

                                        continue;
                                    }

                                    if (propInfo.PropertyType == typeof(DateTimeOffset) ||
                                        propInfo.PropertyType == typeof(DateTimeOffset?))
                                    {
                                        propInfo.SetValue(deserializedEntity, serializedProp.TimestampValue.ToDateTimeOffset(),
                                            null);
                                    }

                                    continue;
                                case Value.ValueTypeOneofCase.DoubleValue:
                                    if (propInfo.PropertyType == typeof(double) ||
                                        propInfo.PropertyType == typeof(double?))
                                    {
                                        propInfo.SetValue(deserializedEntity, serializedProp.DoubleValue, null);
                                    }

                                    continue;
                                case Value.ValueTypeOneofCase.IntegerValue:
                                    if (propInfo.PropertyType == typeof(int) || propInfo.PropertyType == typeof(int?))
                                    {
                                        propInfo.SetValue(deserializedEntity, Convert.ToInt32(serializedProp.IntegerValue), null);

                                        continue;
                                    }

                                    if (propInfo.PropertyType == typeof(long) || propInfo.PropertyType == typeof(long?))
                                    {
                                        propInfo.SetValue(deserializedEntity, serializedProp.IntegerValue, null);
                                    }

                                    continue;
                                default:
                                    continue;
                            }
                        }
                    }
                }
            }

            deserializedEntity.PartitionKey = entity.Key.Path.First().Kind;
            deserializedEntity.RowKey = entity.Key.Path.First().Name;

            return deserializedEntity;
        }

        private static bool ShouldSkipProperty(PropertyInfo property, ILogger logger, string[] columnsToProjection = null)
        {
            var name = property.Name;

            if (name == TableEntityCommonProperties.PartitionKey 
                || name == TableEntityCommonProperties.RowKey)
            {
                return true;
            }

            if (columnsToProjection != null && columnsToProjection.Any() && !columnsToProjection.Contains(name))
            {
                return true;
            }

            var setProp = property.GetSetMethod();
            var getProp = property.GetGetMethod();

            if (setProp == null || !setProp.IsPublic || getProp == null || !getProp.IsPublic)
            {
                logger?.LogInformation("Omitting property '{0}' from serialization/de-serialization because the property's getter/setter are not public.", property.Name);

                return true;
            }

            if (setProp.IsStatic)
            {
                return true;
            }

            if (!Attribute.IsDefined(property, typeof(IgnorePropertyAttribute)))
            {
                return false;
            }

            logger.LogInformation("Omitting property '{0}' from serialization/de-serialization because IgnoreAttribute has been set on that property.", property.Name);

            return true;
        }

        private static Value CreatePropertyFromObject(
            object value,
            Type type)
        {
            // Supported by Azure Table types
            // https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#property-types
            // Supported by Google Datastore types
            // https://cloud.google.com/datastore/docs/concepts/entities#properties_and_value_types
            if (type == typeof(string))
            {
                Value result = (string)value;

                // https://social.msdn.microsoft.com/Forums/vstudio/en-US/053aa028-774c-4a81-9586-16cb0e469177/how-to-know-the-byte-size-of-a-string?forum=csharpgeneral
                if (result != null && 2 * result.StringValue.Length > IndexedPropertyLimit)
                {
                    result.ExcludeFromIndexes = true;
                }

                return result;
            }

            if (type == typeof(byte[]))
            {
                Value result = (byte[])value;

                if (result != null && result.BlobValue.Length > IndexedPropertyLimit)
                {
                    result.ExcludeFromIndexes = true;
                }

                return result;
            }

            if (type == typeof(bool))
            {
                return (bool)value;
            }

            if (type == typeof(bool?))
            {
                return (bool?)value;
            }

            if (type == typeof(DateTime))
            {
                return (DateTime)value;
            }

            if (type == typeof(DateTime?))
            {
                return (DateTime?)value;
            }

            if (type == typeof(DateTimeOffset))
            {
                return (DateTimeOffset)value;
            }

            if (type == typeof(DateTimeOffset?))
            {
                return (DateTimeOffset?)value;
            }

            if (type == typeof(double))
            {
                return (double)value;
            }

            if (type == typeof(double?))
            {
                return (double?)value;
            }

            if (type == typeof(Guid?))
            {
                return value?.ToString();
            }

            if (type == typeof(Guid))
            {
                return value.ToString();
            }

            if (type == typeof(int))
            {
                return (int)value;
            }

            if (type == typeof(int?))
            {
                return (int?)value;
            }

            if (type == typeof(long))
            {
                return (long)value;
            }

            return type == typeof(long?) ? (long?)value : null;
        }
    }
}
