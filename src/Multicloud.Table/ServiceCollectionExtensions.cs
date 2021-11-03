using Multicloud.Table.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Linq;
using System.Reflection;

namespace Multicloud.Table
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddCloudStorageTable(this IServiceCollection serviceCollection, Action<StorageTableOptions> configure = null)
        {
            return serviceCollection
                .AddSingleton(sp =>
                {
                    var opt = new StorageTableOptions();

                    try
                    {
                        configure?.Invoke(opt);
                    }
                    catch
                    {
                        // Just swallow, options can't be initialized
                        // use default settings
                        // due to the caller's handler throws exception
                    }

                    return opt;
                })
                .AddSingleton<ITableClientFactory, TableClientFactory>()
                .AddSingleton(sp =>
                {
                    var tableClientTypes = Assembly
                        .GetExecutingAssembly()
                        .GetTypes()
                        .Where(x => x.IsClass
                                    && !x.IsAbstract
                                    && typeof(ITableClient).IsAssignableFrom(x)
                                    && x.GetCustomAttributes<TableProviderAttribute>().Any())
                        .ToDictionary(
                            x => x.GetCustomAttributes<TableProviderAttribute>().First().Provider,
                            x => x);

                    return new Func<TableProviderOptions, ITableClient>(options =>
                    {
                        if (options == null)
                        {
                            throw new ArgumentNullException(nameof(options));
                        }

                        if (!tableClientTypes.TryGetValue(options.Provider, out var tableClientType))
                        {
                            throw new ArgumentException(
                                $"There is no table client type connected to provider: {options.Provider}. Check provider name.");
                        }

                        if (options.Options == null)
                        {
                            throw new ArgumentException($"{nameof(options.Options)} can't be null.");
                        }

                        return (ITableClient)ActivatorUtilities.CreateInstance(sp, tableClientType, options.Options);
                    });
                });
        }
    }
}
