using System.Linq.Expressions;
using System.Text;

namespace Argentini.DataStore;

/// <summary>
/// DsQuery is used to create filtering rules for returned results.
/// </summary>
/// <example>
/// <code>
/// <![CDATA[
///     var dsQuery = new DsQuery("Manager")
///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
///         .AND()
///         .GroupBegin()
///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
///             .OR()
///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
///         .GroupEnd()
///     );
/// ]]>
/// </code>
/// </example>
public class DsQuery
{
    #region Constructors

    /// <summary>
    /// Instantiate a new DsQuery object.
    /// </summary>
    /// <param name="objectTypes"></param>
    /// <param name="parentId"></param>
    // ReSharper disable once MemberCanBePrivate.Global
    public DsQuery(Guid? parentId = null)
    {
        ParentId = parentId;
    }

    /// <summary>
    /// Instantiate a new DsQuery object.
    /// </summary>
    /// <param name="dsQuery"></param>
    public DsQuery(DsQuery? dsQuery)
    {
        if (dsQuery != null)
        {
            dsQuery.CloneObjectTo(this);
        }

        else
        {
            throw new Exception("DataStore.DsQuery() => Source DsQuery object cannot be null");
        }
    }

    #endregion

    #region Properties

    public bool HasQuery => Query.SbHasValue();
    public bool ObjectIsDeleted { get; set; }
    public string LastPropertyName { get; set; } = string.Empty;
    public string LastFieldName { get; set; } = string.Empty;
    
    public Guid? ParentId { get; set; }
    
    private readonly StringBuilder _whereClause = new ();
    public readonly StringBuilder Query = new ();

    public readonly Dictionary<string,string> CrossApplyWithFields = new ();
    public readonly Dictionary<string,string> CrossApplyWithFieldNames = new ();

    /// <summary>
    /// Generated SQL WHERE clause
    /// </summary>
    public string WhereClause
    {
        get
        {
            _whereClause.Clear();

            if (ObjectIsDeleted)
            {
                _whereClause.Append("[IsDeleted] = 1");
            }

            else
            {
                _whereClause.Append("[IsDeleted] = 0");
            }
            
            if (ParentId != null)
            {
                _whereClause.Append($" AND [ParentId] = '{ParentId.Value}'");
            }

            if (HasQuery)
            {
                _whereClause.Append(" AND ");
                _whereClause.Append(Query);
            }

            return _whereClause.ToString();
        }
    }
    
    #endregion

    #region Public Methods

    #region Conjunctions and Grouping

    /// <summary>
    /// Add a SQL AND to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery AND()
    {
        if (Query.SbEndsWith(" AND ") == false && Query.SbEndsWith(" OR ") == false)
        {
            Query.Append(" AND ");
        }

        return this;
    }

    /// <summary>
    /// Add a SQL OR to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery OR()
    {
        if (Query.SbEndsWith(" AND ") == false && Query.SbEndsWith(" OR ") == false)
        {
            Query.Append(" OR ");
        }

        return this;
    }

    /// <summary>
    /// Add a SQL NOT to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery NOT()
    {
        if (Query.SbEndsWith(" ") == false) Query.Append(' ');

        Query.Append("NOT ");

        return this;
    }
    
    /// <summary>
    /// Add a grouping "(" character to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery GroupBegin()
    {
        Query.Append('(');

        return this;
    }

    /// <summary>
    /// Add a grouping close ")" character to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery GroupEnd()
    {
        Query.Append(')');

        return this;
    }

    /// <summary>
    /// Add a NOT grouping "NOT (" character to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery NotBegin()
    {
        Query.Append("NOT (");

        return this;
    }

    /// <summary>
    /// Add a NOT grouping close ")" character to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery NotEnd()
    {
        Query.Append(')');

        return this;
    }
    
    #endregion
    
    #region Utility
    
    /// <summary>
    /// Reset the DsQuery object and start over (for object reuse and chaining).
    /// </summary>
    /// <returns></returns>
    public DsQuery Clear()
    {
        _whereClause.Clear();
        Query.Clear();

        return this;
    }

    #endregion
    
    #region Standard Properties

    #region Id
    
    /// <summary>
    /// Add 'Id equal to' to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery IdEqualTo(Guid? id)
    {
        if (id != null) Query.Append($"[Id] = '{id.Value}'");
        
        return this;
    }

    /// <summary>
    /// Add an "Id is in list" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery IdInList(List<Guid>? list)
    {
        if (list != null && list.Any())
        {
            Query.Append($"[Id] IN ('{string.Join("','", list)}')");
        }

        else
        {
            Query.Append($"[Id] IN ('{Guid.NewGuid()}')");
        }            
        
        return this;
    }

    /// <summary>
    /// Add an "Id is in list" comparison to the WHERE clause.
    /// Uses a comma-separated list.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery IdInList(string list)
    {
        var splits = list.Split(',');
        
        if (list.Any())
        {
            Query.Append($"[Id] IN ('{string.Join("','", splits)}')");
        }

        else
        {
            Query.Append($"[Id] IN ('{Guid.NewGuid()}')");
        }

        return this;
    }

    #endregion
    
    #region Parent Id
    
    /// <summary>
    /// Add 'ParentId equal to' to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery ParentIdEqualTo(Guid? id)
    {
        if (id != null) Query.Append($"[ParentId] = '{id.Value}'");
        
        return this;
    }

    /// <summary>
    /// Add 'ParentId is null' to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery ParentIdIsNull()
    {
        Query.Append("[ParentId] IS NULL");
        
        return this;
    }
    
    /// <summary>
    /// Add an "ParentId is in list" comparison to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery ParentIdInList(List<Guid> list)
    {
        if (list.Any())
        {
            Query.Append($"[ParentId] IN ('{string.Join("','", list)}')");
        }

        else
        {
            Query.Append($"[ParentId] IN ('{Guid.NewGuid()}')");
        }            
        
        return this;
    }

    /// <summary>
    /// Add an "ParentId is in list" comparison to the WHERE clause.
    /// Uses a comma-separated list.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery ParentIdInList(string list)
    {
        var splits = list.Split(',');
        
        if (list.Any())
        {
            Query.Append($"[ParentId] IN ('{string.Join("','", splits)}')");
        }

        else
        {
            Query.Append($"[ParentId] IN ('{Guid.NewGuid()}')");
        }

        return this;
    }

    #endregion

    #region Lineage
    
    /// <summary>
    /// Add a 'has a specific ancestor' check to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery HasAncestor<T>(Guid? ancestorId)
    {
        if (ancestorId != null)
        {
            Query.Append($"[dbo].[udf_datastore_{typeof(T).Name}_HasAncestor]([Id], '{ancestorId}') = 1");
        }

        return this;
    }

    /// <summary>
    /// Add 'depth equal to' to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery DepthEqualTo(int depth)
    {
        Query.Append($"[Depth] = {depth}");

        return this;
    }

    /// <summary>
    /// Add 'depth less than or equal' to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery DepthLessThanOrEqual(int depth)
    {
        Query.Append($"[Depth] <= {depth}");

        return this;
    }
   
    /// <summary>
    /// Add 'depth less than' to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery DepthLessThan(int depth)
    {
        Query.Append($"[Depth] < {depth}");

        return this;
    }

    /// <summary>
    /// Add 'depth greater than or equal' to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery DepthGreaterThanOrEqual(int depth)
    {
        Query.Append($"[Depth] >= {depth}");

        return this;
    }
    
    /// <summary>
    /// Add 'depth greater than' to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery DepthGreaterThan(int depth)
    {
        Query.Append($"[Depth] > {depth}");

        return this;
    }

    /// <summary>
    /// Add a 'depth inclusive between' to the WHERE clause.
    /// </summary>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery DepthBetween(int startDepth, int endDepth)
    {
        Query.Append($"[Depth] BETWEEN {startDepth} AND {endDepth}");

        return this;
    }
    
    #endregion

    #region Booleans

    /// <summary>
    /// Query for objects marked deleted.
    /// </summary>
    /// <param name="value"></param>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery IsDeleted()
    {
        ObjectIsDeleted = true;

        return this;
    }

    /// <summary>
    /// Query for objects not marked deleted.
    /// </summary>
    /// <param name="value"></param>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery IsNotDeleted()
    {
        ObjectIsDeleted = false;

        return this;
    }
    
    #endregion
    
    #endregion

    #region Custom Properties

    #region Property Helpers
    
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

    /// <summary>
    /// Add a DsProperty field to the WHERE clause.
    /// </summary>
    /// <param name="propertyName">Name of the DsProperty</param>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsQuery Prop(string propertyName)
    {
        LastFieldName = propertyName;
        
        Query.Append($"[{propertyName}]");

        return this;
    }

    #endregion
    
    #region BoolProp
    
    public DsQueryBoolPropComparison BoolProp(string propName)
    {
        return new DsQueryBoolProp(this).BoolProp(propName);
    }

    public DsQueryBoolPropComparison BoolProp<T>(Expression<Func<T, object?>> expression)
    {
        return new DsQueryBoolProp(this).BoolProp(DsObject.GetJsonPropertyPath(expression));
    }    

    public DsQueryBoolPropComparison BoolProp<T,U>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expression)
    {
        return new DsQueryBoolProp(this).BoolProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expression));
    }    

    public DsQueryBoolPropComparison BoolProp<T,U,V>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expression)
    {
        return new DsQueryBoolProp(this).BoolProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expression));
    }    

    public DsQueryBoolPropComparison BoolProp<T,U,V,W>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expression)
    {
        return new DsQueryBoolProp(this).BoolProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expression));
    }    

    public DsQueryBoolPropComparison BoolProp<T,U,V,W,X>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expression)
    {
        return new DsQueryBoolProp(this).BoolProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expression));
    }    

    public DsQueryBoolPropComparison BoolProp<T,U,V,W,X,Y>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expressionPrefix5, Expression<Func<Y, object?>> expression)
    {
        return new DsQueryBoolProp(this).BoolProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expressionPrefix5, expression));
    }    
    
    #endregion
    
    #region DateTimeProp
    
    public DsQueryDateTimePropComparison DateTimeProp(string propName)
    {
        return new DsQueryDateTimeProp(this).DateTimeProp(propName);
    }

    public DsQueryDateTimePropComparison DateTimeProp<T>(Expression<Func<T, object?>> expression)
    {
        return new DsQueryDateTimeProp(this).DateTimeProp(DsObject.GetJsonPropertyPath(expression));
    }

    public DsQueryDateTimePropComparison DateTimeProp<T,U>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expression)
    {
        return new DsQueryDateTimeProp(this).DateTimeProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expression));
    }    

    public DsQueryDateTimePropComparison DateTimeProp<T,U,V>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expression)
    {
        return new DsQueryDateTimeProp(this).DateTimeProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expression));
    }    

    public DsQueryDateTimePropComparison DateTimeProp<T,U,V,W>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expression)
    {
        return new DsQueryDateTimeProp(this).DateTimeProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expression));
    }    

    public DsQueryDateTimePropComparison DateTimeProp<T,U,V,W,X>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expression)
    {
        return new DsQueryDateTimeProp(this).DateTimeProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expression));
    }    

    public DsQueryDateTimePropComparison DateTimeProp<T,U,V,W,X,Y>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expressionPrefix5, Expression<Func<Y, object?>> expression)
    {
        return new DsQueryDateTimeProp(this).DateTimeProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expressionPrefix5, expression));
    }    
    
    #endregion
    
    #region GuidProp
    
    public DsQueryGuidPropComparison GuidProp(string propName)
    {
        return new DsQueryGuidProp(this).GuidProp(propName);
    }

    public DsQueryGuidPropComparison GuidProp<T>(Expression<Func<T, object?>> expression)
    {
        return new DsQueryGuidProp(this).GuidProp(DsObject.GetJsonPropertyPath(expression));
    }    

    public DsQueryGuidPropComparison GuidProp<T,U>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expression)
    {
        return new DsQueryGuidProp(this).GuidProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expression));
    }    

    public DsQueryGuidPropComparison GuidProp<T,U,V>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expression)
    {
        return new DsQueryGuidProp(this).GuidProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expression));
    }    

    public DsQueryGuidPropComparison GuidProp<T,U,V,W>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expression)
    {
        return new DsQueryGuidProp(this).GuidProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expression));
    }    

    public DsQueryGuidPropComparison GuidProp<T,U,V,W,X>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expression)
    {
        return new DsQueryGuidProp(this).GuidProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expression));
    }    

    public DsQueryGuidPropComparison GuidProp<T,U,V,W,X,Y>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expressionPrefix5, Expression<Func<Y, object?>> expression)
    {
        return new DsQueryGuidProp(this).GuidProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expressionPrefix5, expression));
    }    
    
    #endregion
    
    #region StringProp
    
    public DsQueryStringPropComparison StringProp(string propName)
    {
        return new DsQueryStringProp(this).StringProp(propName);
    }

    public DsQueryStringPropComparison StringProp<T>(Expression<Func<T, object?>> expression)
    {
        return new DsQueryStringProp(this).StringProp(DsObject.GetJsonPropertyPath(expression));
    }    

    public DsQueryStringPropComparison StringProp<T,U>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expression)
    {
        return new DsQueryStringProp(this).StringProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expression));
    }    

    public DsQueryStringPropComparison StringProp<T,U,V>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expression)
    {
        return new DsQueryStringProp(this).StringProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expression));
    }    

    public DsQueryStringPropComparison StringProp<T,U,V,W>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expression)
    {
        return new DsQueryStringProp(this).StringProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expression));
    }    

    public DsQueryStringPropComparison StringProp<T,U,V,W,X>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expression)
    {
        return new DsQueryStringProp(this).StringProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expression));
    }    

    public DsQueryStringPropComparison StringProp<T,U,V,W,X,Y>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expressionPrefix5, Expression<Func<Y, object?>> expression)
    {
        return new DsQueryStringProp(this).StringProp(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expressionPrefix5, expression));
    }    
    
    #endregion

    #region NumberProp
    
    public DsQueryNumberPropComparison NumberProp<T>(string propName)
    {
        return new DsQueryNumberProp(this).NumberProp<T>(propName);
    }

    public DsQueryNumberPropComparison NumberProp<T,U>(Expression<Func<T, object?>> expression)
    {
        return new DsQueryNumberProp(this).NumberProp<U>(DsObject.GetJsonPropertyPath(expression));
    }

    public DsQueryNumberPropComparison NumberProp<T,U,V>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expression)
    {
        return new DsQueryNumberProp(this).NumberProp<V>(DsObject.GetJsonPropertyPath(expressionPrefix1, expression));
    }    

    public DsQueryNumberPropComparison NumberProp<T,U,V,W>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expression)
    {
        return new DsQueryNumberProp(this).NumberProp<W>(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expression));
    }    

    public DsQueryNumberPropComparison NumberProp<T,U,V,W,X>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expression)
    {
        return new DsQueryNumberProp(this).NumberProp<X>(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expression));
    }    

    public DsQueryNumberPropComparison NumberProp<T,U,V,W,X,Y>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expression)
    {
        return new DsQueryNumberProp(this).NumberProp<Y>(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expression));
    }    

    public DsQueryNumberPropComparison NumberProp<T,U,V,W,X,Y,Z>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expressionPrefix5, Expression<Func<Y, object?>> expression)
    {
        return new DsQueryNumberProp(this).NumberProp<Z>(DsObject.GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expressionPrefix5, expression));
    }    
    
    #endregion

    #endregion

    #endregion
}
