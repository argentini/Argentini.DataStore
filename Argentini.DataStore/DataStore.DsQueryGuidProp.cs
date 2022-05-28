namespace Argentini.DataStore;

public class DsQueryGuidProp
{
    private DsQuery Query { get; }

    public DsQueryGuidProp(DsQuery query)
    {
        Query = query;
    }

    /// <summary>
    /// Add a GUID field to the WHERE clause.
    /// </summary>
    /// <param name="propertyName">Name of the DsProperty</param>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQueryGuidPropComparison GuidProp(string propertyName)
    {
        var isNewItem = Query.AddCrossApplyWithFieldName(propertyName);
        var fieldName = Query.CrossApplyWithFieldNames[propertyName];

        Query.LastPropertyName = propertyName;
        Query.LastFieldName = fieldName;
        Query.Query.Append($"[{fieldName}]");

        var comparison = new DsQueryGuidPropComparison(Query);

        if (isNewItem)
            comparison.AddCrossApplyWithField<string>();

        return comparison;
    }
}

public class DsQueryGuidPropComparison: DsQueryPropComparisonBase
{
    public DsQueryGuidPropComparison(DsQuery query) : base(query)
    {
    }

    #region Comparisons

    /// <summary>
    /// Add a custom condition to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery RawSql(string value)
    {
        Query.Query.Append(value);

        return Query;
    }
    
    /// <summary>
    /// Add an "equal to" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery EqualTo(Guid value)
    {
        Query.Query.Append($" = {ProcessValue(value)}");

        return Query;
    }

    /// <summary>
    /// Add an "is null" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery IsNull()
    {
        Query.Query.Append(" IS NULL");
        
        return Query;
    }

    /// <summary>
    /// Add an "GUID is in list" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery InList(IEnumerable<Guid> list)
    {
        var enumerable = list as Guid[] ?? list.ToArray();

        if (enumerable.Any())
        {
            Query.Query.Append($" IN ('{string.Join("','", enumerable)}')");
        }

        else
        {
            Query.Query.Append($" IN ('{Guid.NewGuid()}')");
        }

        return Query;
    }

    /// <summary>
    /// Add an "GUID is in list" comparison to the WHERE clause.
    /// Uses a comma-separated list.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery InList(string list)
    {
        var splits = list.Split(',');
        
        if (list.Any())
        {
            Query.Query.Append($" IN ('{string.Join("','", splits)}')");
        }

        else
        {
            Query.Query.Append($" IN ('{Guid.NewGuid()}')");
        }

        return Query;
    }
    
    #endregion

    #region Utilities
    
    /// <summary>
    /// Format an object value for use in the SQL WHERE clause.
    /// </summary>
    /// <param name="value">Value to format</param>
    /// <returns>Value formatted for use in the SQL WHERE clause</returns>
    private static string ProcessValue(Guid value)
    {
        return "'" + value + "'";
    }
    
    #endregion
}
