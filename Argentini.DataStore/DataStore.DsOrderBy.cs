using System.Linq.Expressions;
using System.Text;

namespace Argentini.DataStore;

/// <summary>
/// DsOrderBy is used to create sorting rules for returned results.
/// </summary>
/// <example>
/// Remember: If you order by a property in a list (e.g. a list of email addresses),
/// your paging results will contain duplicate object entries (one object for each of its email addresses).
/// So like any SQL statement, you should order by single field values.
/// <code>
/// <![CDATA[
///     var dsOrderby = new DsOrderBy()
///         .DepthAscending()
///         .Prop<User,int>(p => p.Age).Ascending()
///     );
/// ]]>
/// </code>
/// </example>
public class DsOrderBy
{
    #region Properties

    public readonly Dictionary<string,string> CrossApplyWithFields = new();
    public readonly Dictionary<string,string> CrossApplyWithFieldNames = new();
    public readonly List<string> FieldNames = new ();
    public string LastPropertyName { get; set; } = string.Empty;
    public string LastFieldName { get; set; } = string.Empty;
    
    public readonly StringBuilder OrderBy = new ();
    private readonly StringBuilder _orderByClause = new ();

    /// <summary>
    /// Generated SQL ORDER BY clause
    /// </summary>
    public string OrderByClause
    {
        get
        {
            _orderByClause.Clear();
                
            if (OrderBy.SbHasValue())
            {
                _orderByClause.Append(OrderBy);
            }

            if (_orderByClause.SbIsEmpty())
            {
                FieldNames.Add("Sort");
                _orderByClause.Append("[Sort] ASC");
            }

            return _orderByClause.ToString();
        }
    }
    
    #endregion
    
    #region Custom Properties

    /// <summary>
    /// Adds the property path to a dictionary;
    /// Keeps track of JSON paths and SQL field names used in queries and ordering.
    /// <example>
    /// A propName of "features.emails.email" stores 
    /// Key = features.emails.email
    /// Value = features_emails_email
    /// </example>
    /// </summary>
    /// <param name="propName"></param>
    public bool AddCrossApplyWithFieldName(string propName)
    {
        if (CrossApplyWithFieldNames.ContainsKey(propName)) return false;

        CrossApplyWithFieldNames.Add(propName, DataStore.JsonPathToColumnName(propName));

        return true;
    }

    public DsOrderByPropDirection Prop(string propName) => Prop<string>(propName);
    public DsOrderByPropDirection Prop<T>(string propName)
    {
        if (FieldNames.Contains(propName) == false) FieldNames.Add(propName);
        return new DsOrderByProp(this).Prop<T>(propName);
    }

    public DsOrderByPropDirection Prop<T>(Expression<Func<T, object?>> expression) => Prop<T,string>(expression); 
    public DsOrderByPropDirection Prop<T,U>(Expression<Func<T, object?>> expression)
    {
        var propName = DataStore.JsonPathToColumnName(DsObject.GetJsonPropertyPath(expression));
        if (FieldNames.Contains(propName) == false) FieldNames.Add(propName);

        return new DsOrderByProp(this).Prop<U>(DsObject.GetJsonPropertyPath(expression));
    }

    #endregion
    
    #region Standard Properties 
    
    public void EnsureComma()
    {
        if (OrderBy.SbHasValue())
        {
            OrderBy.Append(", ");
        }
    }
    
    /// <summary>
    /// Add 'order by object sort, ascending' to the order by clause.
    /// </summary>
    /// <returns>Current DsOrderBy object (for chaining)</returns>
    public DsOrderBy SortAscending()
    {
        if (FieldNames.Contains("Sort") == false) FieldNames.Add("Sort");

        EnsureComma();
        OrderBy.Append("[Sort] ASC");

        return this;
    }

    /// <summary>
    /// Add 'order by object sort, descending' to the order by clause.
    /// </summary>
    /// <returns>Current DsOrderBy object (for chaining)</returns>
    public DsOrderBy SortDescending()
    {
        if (FieldNames.Contains("Sort") == false) FieldNames.Add("Sort");

        EnsureComma();
        OrderBy.Append("[Sort] DESC");

        return this;
    }
    
    /// <summary>
    /// Add 'order by object depth, ascending' to the order by clause.
    /// </summary>
    /// <returns>Current DsOrderBy object (for chaining)</returns>
    public DsOrderBy DepthAscending()
    {
        if (FieldNames.Contains("Depth") == false) FieldNames.Add("Depth");

        EnsureComma();
        OrderBy.Append("[Depth] ASC");

        return this;
    }

    /// <summary>
    /// Add 'order by object depth, descending' to the order by clause.
    /// </summary>
    /// <returns>Current DsOrderBy object (for chaining)</returns>
    public DsOrderBy DepthDescending()
    {
        if (FieldNames.Contains("Depth") == false) FieldNames.Add("Depth");

        EnsureComma();
        OrderBy.Append("[Depth] DESC");

        return this;
    }
    
    #endregion
    
    #region Utility Methods

    /// <summary>
    /// Reset the DsOrderBy object and start over (for object reuse and chaining).
    /// </summary>
    /// <returns></returns>
    public DsOrderBy Clear()
    {
        _orderByClause.Clear();
        OrderBy.Clear();

        return this;
    }

    #endregion
}
