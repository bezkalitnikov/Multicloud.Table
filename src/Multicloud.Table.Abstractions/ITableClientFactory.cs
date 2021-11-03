namespace Multicloud.Table.Abstractions
{
    public interface ITableClientFactory
    {
        ITableClient Create(TableProviderOptions options);
    }
}
