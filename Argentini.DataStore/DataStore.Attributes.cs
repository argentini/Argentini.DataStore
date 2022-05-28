namespace Argentini.DataStore;

[AttributeUsage(AttributeTargets.Class)]
public class DsUseLineageFeatures : Attribute { }

[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public class DsNoDatabaseTable : Attribute { }

[AttributeUsage(AttributeTargets.Class)]
public class DsSerializerContext : Attribute
{
    public Type[] Values { get; }

    public DsSerializerContext(params Type[] values)
    {
        Values = values;
    }
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class DsIndexedColumn : Attribute
{
    public string[] Value { get; }

    public DsIndexedColumn(params string[] keyNames)
    {
        Value = keyNames;
    }

    public DsIndexedColumn()
    {
        Value = Array.Empty<string>();
    }
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class DsUniqueIndex : Attribute
{
    public Type[] Values { get; }

    public DsUniqueIndex(params Type[] values)
    {
        Values = values;
    }
}

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public class DsClusteredIndex : Attribute
{
    public Type[] Values { get; }

    public DsClusteredIndex(params Type[] values)
    {
        Values = values;
    }
}
