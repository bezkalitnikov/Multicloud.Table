using Multicloud.Table.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;

namespace Multicloud.Table
{
    public class TableClientFactory : ITableClientFactory
    {
        private readonly Func<TableProviderOptions, ITableClient> _tableClientFactory;

        private readonly ILogger<TableClientFactory> _logger = new NullLogger<TableClientFactory>();

        public TableClientFactory(Func<TableProviderOptions, ITableClient> tableClientFactory,
            StorageTableOptions storageTableOptions, ILoggerFactory loggerFactory = null)
        {
            _tableClientFactory = tableClientFactory ?? throw new ArgumentNullException(nameof(tableClientFactory));

            if (storageTableOptions == null)
            {
                throw new ArgumentNullException(nameof(storageTableOptions));
            }

            if (storageTableOptions.EnableLogging && loggerFactory != null)
            {
                _logger = loggerFactory.CreateLogger<TableClientFactory>();
            }
        }

        public ITableClient Create(TableProviderOptions providerOptions)
        {
            return _tableClientFactory(providerOptions);
        }
    }
}
