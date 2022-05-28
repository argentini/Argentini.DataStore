namespace Argentini.DataStore;

public class DsQueryBoolProp
{
    private DsQuery Query { get; }

    public DsQueryBoolProp(DsQuery query)
    {
        Query = query;
    }

    /// <summary>
    /// Add a boolean field to the WHERE clause.
    /// </summary>
    /// <param name="propertyName">Name of the DsProperty</param>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQueryBoolPropComparison BoolProp(string propertyName)
    {
        var isNewItem = Query.AddCrossApplyWithFieldName(propertyName);
        var fieldName = Query.CrossApplyWithFieldNames[propertyName];

        Query.LastPropertyName = propertyName;
        Query.LastFieldName = fieldName;
        Query.Query.Append($"[{fieldName}]");

        var comparison = new DsQueryBoolPropComparison(Query);

        if (isNewItem)
            comparison.AddCrossApplyWithField<string>();

        return comparison;
    }
}

public class DsQueryBoolPropComparison: DsQueryPropComparisonBase
{
    public DsQueryBoolPropComparison(DsQuery query) : base(query)
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
    public DsQuery EqualTo(bool value)
    {
        Query.Query.Append($" = {ProcessValue(value)}");
        
        return Query;
    }

    /// <summary>
    /// Add a boolean comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery IsTrue()
    {
        Query.Query.Append(" = 1");
        
        return Query;
    }

    /// <summary>
    /// Add a boolean comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery IsFalse()
    {
        Query.Query.Append(" = 0");
        
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
    
    #endregion

    #region Utilities
    
    /// <summary>
    /// Format an object value for use in the SQL WHERE clause.
    /// </summary>
    /// <param name="value">Value to format</param>
    /// <returns>Value formatted for use in the SQL WHERE clause</returns>
    private static string ProcessValue(bool value)
    {
        return value ? "1" : "0";
    }
    
    #endregion
}
