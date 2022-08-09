namespace DataStore;

public class DsQueryNumberProp
{
    private DsQuery Query { get; }

    public DsQueryNumberProp(DsQuery query)
    {
        Query = query;
    }

    /// <summary>
    /// Add a decimal field to the WHERE clause.
    /// </summary>
    /// <param name="propertyName">Name of the DsProperty</param>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQueryNumberPropComparison NumberProp<T>(string propertyName)
    {
        var isNewItem = Query.AddCrossApplyWithFieldName(propertyName);
        var fieldName = Query.CrossApplyWithFieldNames[propertyName];

        Query.LastPropertyName = propertyName;
        Query.LastFieldName = fieldName;
        Query.Query.Append($"[{fieldName}]");

        var comparison = new DsQueryNumberPropComparison(Query);

        if (isNewItem)
            comparison.AddCrossApplyWithField<T>();

        return comparison;
    }
}

public class DsQueryNumberPropComparison: DsQueryPropComparisonBase
{
    public DsQueryNumberPropComparison(DsQuery query) : base(query)
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
    public DsQuery EqualTo<T>(T value)
    {
        Query.Query.Append($" = {ProcessValue(value)}");

        return Query;
    }

    /// <summary>
    /// Add a "less than or equal to" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery LessThanOrEqualTo<T>(T value)
    {
        Query.Query.Append($" <= {ProcessValue(value)}");

        return Query;
    }
    
    /// <summary>
    /// Add a "less than" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery LessThan<T>(T value)
    {
        Query.Query.Append($" < {ProcessValue(value)}");

        return Query;
    }

    /// <summary>
    /// Add a "greater than or equal to" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery GreaterThanOrEqualTo<T>(T value)
    {
        Query.Query.Append($" >= {ProcessValue(value)}");

        return Query;
    }
    
    /// <summary>
    /// Add a "greater than" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery GreaterThan<T>(T value)
    {
        Query.Query.Append($" > {ProcessValue(value)}");

        return Query;
    }
    
    /// <summary>
    /// Add an inclusive "between" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery Between<T>(T startValue, T endValue)
    {
        Query.Query.Append($" BETWEEN {ProcessValue(startValue)} AND {ProcessValue(endValue)}");

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
    private static string ProcessValue<T>(T value)
    {
        return value?.ToString() ?? string.Empty;
    }
    
    #endregion
}
