namespace DataStore;

public class DsQueryDateTimeProp
{
    private DsQuery Query { get; }

    public DsQueryDateTimeProp(DsQuery query)
    {
        Query = query;
    }

    /// <summary>
    /// Add a DateTime field to the WHERE clause.
    /// </summary>
    /// <param name="propertyName">Name of the DsProperty</param>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQueryDateTimePropComparison DateTimeProp(string propertyName)
    {
        var isNewItem = Query.AddCrossApplyWithFieldName(propertyName);
        var fieldName = Query.CrossApplyWithFieldNames[propertyName];

        Query.LastPropertyName = propertyName;
        Query.LastFieldName = fieldName;
        Query.Query.Append($"[{fieldName}]");

        var comparison = new DsQueryDateTimePropComparison(Query);

        if (isNewItem)
            comparison.AddCrossApplyWithField<string>();

        return comparison;
    }
}

public class DsQueryDateTimePropComparison: DsQueryPropComparisonBase
{
    public DsQueryDateTimePropComparison(DsQuery query) : base(query)
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
    public DsQuery EqualTo(DateTime value) => EqualTo(new DateTimeOffset(value));
    public DsQuery EqualTo(DateTimeOffset value)
    {
        Query.Query.Append($" = {ProcessValue(value)}");
        
        return Query;
    }

    public DsQuery EqualTo(DateOnly value)
    {
        Query.Query.Append($" = {ProcessValue(value)}");
        
        return Query;
    }

    public DsQuery EqualTo(TimeOnly value)
    {
        Query.Query.Append($" = {ProcessValue(value)}");
        
        return Query;
    }

    /// <summary>
    /// Add a "before" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery Before(DateTime value) => Before(new DateTimeOffset(value));
    public DsQuery Before(DateTimeOffset value)
    {
        Query.Query.Append($" < {ProcessValue(value)}");

        return Query;
    }

    public DsQuery Before(DateOnly value)
    {
        Query.Query.Append($" < {ProcessValue(value)}");

        return Query;
    }

    public DsQuery Before(TimeOnly value)
    {
        Query.Query.Append($" < {ProcessValue(value)}");

        return Query;
    }
    
    /// <summary>
    /// Add a "before or equal to" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery BeforeOrEqualTo(DateTime value) => BeforeOrEqualTo(new DateTimeOffset(value));
    public DsQuery BeforeOrEqualTo(DateTimeOffset value)
    {
        Query.Query.Append($" <= {ProcessValue(value)}");

        return Query;
    }

    public DsQuery BeforeOrEqualTo(DateOnly value)
    {
        Query.Query.Append($" <= {ProcessValue(value)}");

        return Query;
    }

    public DsQuery BeforeOrEqualTo(TimeOnly value)
    {
        Query.Query.Append($" <= {ProcessValue(value)}");

        return Query;
    }
    
    /// <summary>
    /// Add a "after" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery After(DateTime value) => After(new DateTimeOffset(value));
    public DsQuery After(DateTimeOffset value)
    {
        Query.Query.Append($" > {ProcessValue(value)}");

        return Query;
    }

    public DsQuery After(DateOnly value)
    {
        Query.Query.Append($" > {ProcessValue(value)}");

        return Query;
    }

    public DsQuery After(TimeOnly value)
    {
        Query.Query.Append($" > {ProcessValue(value)}");

        return Query;
    }
    
    /// <summary>
    /// Add a "after or equal to" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery AfterOrEqualTo(DateTime value) => AfterOrEqualTo(new DateTimeOffset(value));
    public DsQuery AfterOrEqualTo(DateTimeOffset value)
    {
        Query.Query.Append($" >= {ProcessValue(value)}");

        return Query;
    }

    public DsQuery AfterOrEqualTo(DateOnly value)
    {
        Query.Query.Append($" >= {ProcessValue(value)}");

        return Query;
    }

    public DsQuery AfterOrEqualTo(TimeOnly value)
    {
        Query.Query.Append($" >= {ProcessValue(value)}");

        return Query;
    }
    
    /// <summary>
    /// Add a "between" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery Between(DateTime startDate, DateTime endDate) => Between(new DateTimeOffset(startDate), new DateTimeOffset(endDate));
    public DsQuery Between(DateTimeOffset startDate, DateTimeOffset endDate)
    {
        Query.Query.Append($" BETWEEN {ProcessValue(startDate)} AND {ProcessValue(endDate)}");

        return Query;
    }

    public DsQuery Between(DateOnly startDate, DateOnly endDate)
    {
        Query.Query.Append($" BETWEEN {ProcessValue(startDate)} AND {ProcessValue(endDate)}");

        return Query;
    }

    public DsQuery Between(TimeOnly startDate, TimeOnly endDate)
    {
        Query.Query.Append($" BETWEEN {ProcessValue(startDate)} AND {ProcessValue(endDate)}");

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
    private static string ProcessValue(DateTimeOffset value)
    {
        return $"'{value:o}'";
    }

    /// <summary>
    /// Format an object value for use in the SQL WHERE clause.
    /// </summary>
    /// <param name="value">Value to format</param>
    /// <returns>Value formatted for use in the SQL WHERE clause</returns>
    private static string ProcessValue(DateOnly value)
    {
        return $"'{value:d}'";
    }

    /// <summary>
    /// Format an object value for use in the SQL WHERE clause.
    /// </summary>
    /// <param name="value">Value to format</param>
    /// <returns>Value formatted for use in the SQL WHERE clause</returns>
    private static string ProcessValue(TimeOnly value)
    {
        return $"'{value:T}'";
    }

    #endregion
}
