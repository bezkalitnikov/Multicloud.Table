using System.Collections.Generic;

namespace Multicloud.Table.Abstractions
{
    public class TableProviderOptions
    {
        public string Provider { get; set; }

        public IReadOnlyDictionary<string, string> Options { get; set; }
    }
}
