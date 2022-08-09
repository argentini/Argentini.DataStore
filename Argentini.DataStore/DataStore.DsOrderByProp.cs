namespace DataStore;

public class DsOrderByProp
{
    private DsOrderBy OrderBy { get; }

    public enum SortDirections
    {
        Ascending = 0,
        Descending = 1
    }
    
    public DsOrderByProp(DsOrderBy orderBy)
    {
        OrderBy = orderBy;
    }

    /// <summary>
    /// Add a number field to the WHERE clause.
    /// </summary>
    /// <param name="propertyName">Name of the DsProperty</param>
    /// <returns>Current DsQuery object (for chaining)</returns>
    public DsOrderByPropDirection Prop<T>(string propertyName)
    {
        var isNewItem = OrderBy.AddCrossApplyWithFieldName(propertyName);
        var fieldName = OrderBy.CrossApplyWithFieldNames[propertyName];

        OrderBy.LastPropertyName = propertyName;
        OrderBy.LastFieldName = fieldName;
        OrderBy.EnsureComma();
        OrderBy.OrderBy.Append($"[{OrderBy.LastFieldName}] ");
            
        var direction = new DsOrderByPropDirection(OrderBy);

        if (isNewItem)
            direction.AddCrossApplyWithField<T>();

        return direction;
    }
}

public class DsOrderByPropDirection
{
    private DsOrderBy OrderBy { get; }

    public DsOrderByPropDirection(DsOrderBy orderBy)
    {
        OrderBy = orderBy;
    }
    
    /// <summary>
    /// Add an ascending order by clause.
    /// </summary>
    /// <returns>Current DsOrderBy object (for chaining)</returns>
    public DsOrderBy Ascending()
    {
        OrderBy.OrderBy.Append("ASC");

        return OrderBy;
    }

    /// <summary>
    /// Add an ascending order by clause.
    /// </summary>
    /// <returns>Current DsOrderBy object (for chaining)</returns>
    public DsOrderBy Descending()
    {
        OrderBy.OrderBy.Append("DESC");

        return OrderBy;
    }

    /// <summary>
    /// Add an order by direction to the clause.
    /// </summary>
    /// <returns>Current DsOrderBy object (for chaining)</returns>
    public DsOrderBy OrderDirectionRaw(string direction = "ASC")
    {
        if (direction.Equals("Ascending", StringComparison.OrdinalIgnoreCase)) direction = "ASC";
        if (direction.Equals("Descending", StringComparison.OrdinalIgnoreCase)) direction = "DESC";
        if (direction.Equals("ASC", StringComparison.OrdinalIgnoreCase) == false && direction.Equals("DESC", StringComparison.OrdinalIgnoreCase) == false) direction = "ASC";
        
        OrderBy.OrderBy.Append(direction);

        return OrderBy;
    }

    /// <summary>
    /// Add an order by direction to the clause.
    /// </summary>
    /// <returns>Current DsOrderBy object (for chaining)</returns>
    public DsOrderBy OrderDirectionRaw(DsOrderByProp.SortDirections direction)
    {
        OrderBy.OrderBy.Append(direction == DsOrderByProp.SortDirections.Ascending ? "ASC" : "DESC");

        return OrderBy;
    }
    
    /// <summary>
    /// Used to build the JSON field mapping for a query
    /// </summary>
    public void AddCrossApplyWithField<T>()
    {
        var jsonFieldName = OrderBy.LastPropertyName.Split('.').Length > 1 ? OrderBy.LastPropertyName.Split('.')[^1] : OrderBy.LastPropertyName;
        string fieldCast;

        jsonFieldName = "$.\"" + jsonFieldName.Replace(".", "\".\"") + "\"";
        
        if (typeof(T) == typeof(byte))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] tinyint '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(short))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] smallint '{jsonFieldName}'";
        }
        
        else if (typeof(T) == typeof(int))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] int '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(long))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] bigint '{jsonFieldName}'";
        }
        
        else if (typeof(T) == typeof(double))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] float '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(decimal))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] decimal(19,5) '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(bool))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] bit '{jsonFieldName}'";
        }
        
        else if (typeof(T) == typeof(Guid))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] uniqueidentifier '{jsonFieldName}'";
        }
        
        else if (typeof(T) == typeof(DateTimeOffset) || typeof(T) == typeof(DateTime))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] datetimeoffset(7) '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(DateOnly))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] date '{jsonFieldName}'";
        }

        else if (typeof(T) == typeof(TimeOnly))
        {
            fieldCast = $"[{OrderBy.LastFieldName}] time '{jsonFieldName}'";
        }
        
        else
        {
            fieldCast = $"[{OrderBy.LastFieldName}] nvarchar(max) '{jsonFieldName}'";
        }

        if (fieldCast != string.Empty)
        {
            OrderBy.CrossApplyWithFields.Add(OrderBy.LastPropertyName, fieldCast);
        }
    }
}
