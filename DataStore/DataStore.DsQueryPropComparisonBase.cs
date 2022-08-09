namespace DataStore;

public class DsQueryPropComparisonBase
{
    public DsQuery Query { get; }

    public DsQueryPropComparisonBase(DsQuery query)
    {
        Query = query;
    }

    #region Utilities
    
    /// <summary>
    /// Used to build the JSON field mapping for a query
    /// </summary>
    public void AddCrossApplyWithField<T>()
    {
        var jsonFieldName = Query.LastPropertyName.Split('.').Length > 1 ? Query.LastPropertyName.Split('.')[^1] : Query.LastPropertyName;
        var fieldCast = string.Empty;

        jsonFieldName = "$.\"" + jsonFieldName.Replace(".", "\".\"") + "\"";
        
        if (typeof(T) == typeof(byte))
        {
            fieldCast = $"[{Query.LastFieldName}] tinyint '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(short))
        {
            fieldCast = $"[{Query.LastFieldName}] smallint '{jsonFieldName}'";
        }
        
        else if (typeof(T) == typeof(int))
        {
            fieldCast = $"[{Query.LastFieldName}] int '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(long))
        {
            fieldCast = $"[{Query.LastFieldName}] bigint '{jsonFieldName}'";
        }
        
        else if (typeof(T) == typeof(double))
        {
            fieldCast = $"[{Query.LastFieldName}] float '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(decimal))
        {
            fieldCast = $"[{Query.LastFieldName}] decimal(19,5) '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(string))
        {
            fieldCast = $"[{Query.LastFieldName}] nvarchar(max) '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(bool))
        {
            fieldCast = $"[{Query.LastFieldName}] bit '{jsonFieldName}'";            
        }

        else if (typeof(T) == typeof(TimeOnly))
        {
            fieldCast = $"[{Query.LastFieldName}] time '{jsonFieldName}'";
        }
        
        else if (typeof(T) == typeof(DateOnly))
        {
            fieldCast = $"[{Query.LastFieldName}] date '{jsonFieldName}'";
        }
        
        else if (typeof(T) == typeof(DateTime) || typeof(T) == typeof(DateTimeOffset))
        {
            fieldCast = $"[{Query.LastFieldName}] datetimeoffset(7) '{jsonFieldName}'";
        }
        
        else if (typeof(T) == typeof(Guid))
        {
            fieldCast = $"[{Query.LastFieldName}] uniqueidentifier '{jsonFieldName}'";            
        }

        if (fieldCast != string.Empty)
        {
            Query.CrossApplyWithFields.Add(Query.LastPropertyName, fieldCast);
        }
    }
    
    #endregion
}
