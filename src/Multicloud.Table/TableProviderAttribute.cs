using System;

namespace Multicloud.Table
{
    [AttributeUsage(AttributeTargets.Class)]
    internal class TableProviderAttribute : Attribute
    {
        public string Provider { get; set; }
    }
}
