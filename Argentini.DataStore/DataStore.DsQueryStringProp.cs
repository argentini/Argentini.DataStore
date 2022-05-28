using System.Linq.Expressions;

namespace Argentini.DataStore;

public class DsQueryStringProp
{
    private DsQuery Query { get; }

    public DsQueryStringProp(DsQuery query)
    {
        Query = query;
    }
    
    /// <summary>
    /// Add a text field to the WHERE clause.
    /// </summary>
    /// <param name="propertyName">Name of the DsProperty</param>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQueryStringPropComparison StringProp(string propertyName)
    {
        var isNewItem = Query.AddCrossApplyWithFieldName(propertyName);
        var fieldName = Query.CrossApplyWithFieldNames[propertyName];

        Query.LastPropertyName = propertyName;
        Query.LastFieldName = fieldName;
        Query.Query.Append($"[{fieldName}]");

        var comparison = new DsQueryStringPropComparison(Query);

        if (isNewItem)
            comparison.AddCrossApplyWithField<string>();

        return comparison;
    }
}

public class DsQueryStringPropComparison: DsQueryPropComparisonBase
{
    public DsQueryStringPropComparison(DsQuery query) : base(query)
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
    public DsQuery EqualTo(string value, bool caseSensitive = false)
    {
        Query.Query.Append($" = '{value.SqlSanitize()}'{(caseSensitive ? " COLLATE Latin1_General_CS_AS" : string.Empty)}");

        return Query;
    }

    #region EqualToProp()
    
    /// <summary>
    /// Add an "equal to" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery EqualToProp(string propName, bool caseSensitive = false)
    {
        Query.Query.Append(" = ");
        Query.StringProp(propName);
        Query.Query.Append(caseSensitive ? " COLLATE Latin1_General_CS_AS" : string.Empty);

        return Query;
    }

    public DsQuery EqualToProp<T>(Expression<Func<T, object?>> expression, bool caseSensitive = false)
    {
        return EqualToProp(DsObject.GetJsonPropertyPath(expression), caseSensitive);
    }

    public DsQuery EqualToProp<T,U>(Expression<Func<T, object?>> expressionPrefix, Expression<Func<U, object?>> expression, bool caseSensitive = false)
    {
        return EqualToProp(DsObject.GetJsonPropertyPath(expressionPrefix, expression), caseSensitive);
    }

    public DsQuery EqualToProp<T,U,V>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expression, bool caseSensitive = false)
    {
        return EqualToProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expression), caseSensitive);
    }

    public DsQuery EqualToProp<T,U,V,W>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expression, bool caseSensitive = false)
    {
        return EqualToProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expression), caseSensitive);
    }

    public DsQuery EqualToProp<T,U,V,W,X>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expression, bool caseSensitive = false)
    {
        return EqualToProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expression), caseSensitive);
    }
    
    public DsQuery EqualToProp<T,U,V,W,X,Y>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expressionPrefix5, Expression<Func<Y, object?>> expression, bool caseSensitive = false)
    {
        return EqualToProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expressionPrefix5, expression), caseSensitive);
    }

    #endregion
    
    /// <summary>
    /// Add a "starts with" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery StartsWith(string value, bool caseSensitive = false)
    {
        Query.Query.Append($" LIKE '{value.SqlSanitize()}%'{(caseSensitive ? " COLLATE Latin1_General_CS_AS" : string.Empty)}");

        return Query;
    }
    
    /// <summary>
    /// Add an "ends with" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery EndsWith(string value, bool caseSensitive = false)
    {
        Query.Query.Append($" LIKE '%{value.SqlSanitize()}'{(caseSensitive ? " COLLATE Latin1_General_CS_AS" : string.Empty)}");

        return Query;
    }
    
    /// <summary>
    /// Add a "contains substring" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery Contains(string value, bool caseSensitive = false)
    {
        Query.Query.Append($" LIKE '%{value.SqlSanitize()}%'{(caseSensitive ? " COLLATE Latin1_General_CS_AS" : string.Empty)}");

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
    /// Add a "has value" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery HasValue()
    {
        Query.Query.TrimEnd($"[{Query.LastFieldName}]");            
        Query.Query.Append($"([{Query.LastFieldName}] IS NOT NULL AND [{Query.LastFieldName}] <> '')");
        
        return Query;
    }

    /// <summary>
    /// Add an "is null or empty" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery IsNullOrEmpty()
    {
        Query.Query.TrimEnd($"[{Query.LastFieldName}]");            
        Query.Query.Append($"([{Query.LastFieldName}] IS NULL OR [{Query.LastFieldName}] = '')");
        
        return Query;
    }
    
    #endregion

    /// <summary>
    /// Format an object value for use in the SQL WHERE clause.
    /// </summary>
    /// <param name="value">Value to format</param>
    /// <returns>Value formatted for use in the SQL WHERE clause</returns>
    public static string ProcessValue(string value)
    {
        return "'" + value.SqlSanitize() + "'";
    }
}
