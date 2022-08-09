using System.Collections;
using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Serialization;
using JsonCons.JsonPath;

namespace DataStore;

/// <summary>
/// Models should inherit from this base class.
/// <example>
/// <code>
/// <![CDATA[
/// [DsUseLineageFeatures]
/// [DsSerializerContext(typeof(UserJsonSerializerContext))]
/// public class User: DsObject
/// {
///     public string Firstname { get; set; }
///     public int Age { get; set; }
///     public string[] MiddleNames { get; set; }
///     public List<Permissions> Permissions { get; set; }
///     ...
/// }
/// ]]>
/// </code>
/// </example>
/// </summary>
public class DsObject : IDisposable
{
    public DsObject()
    {
        Id = Guid.NewGuid();
        Lineage = new List<Guid>();
        IsNew = true;
    }

    #region Properties
    
    /// <summary>
    /// Unique Id for the object
    /// </summary>
    [JsonPropertyOrder(int.MinValue)][DsIndexedColumn, DsUniqueIndex] public Guid Id { get; set; }

    /// <summary>
    /// Id of the parent object if this is a child
    /// </summary>
    [JsonPropertyOrder(int.MinValue + 1)][DsIndexedColumn] public Guid? ParentId { get; set; }

    /// <summary>
    /// Id lineage (parent to progenitor)
    /// </summary>
    [JsonPropertyOrder(int.MinValue + 2)][DsIndexedColumn] public List<Guid>? Lineage { get; set; }

    /// <summary>
    /// Depth value for the object
    /// </summary>
    [JsonPropertyOrder(int.MinValue + 3)][DsIndexedColumn] public int? Depth { get; set; }
    
    /// <summary>
    /// Sort value for the object
    /// </summary>
    [JsonPropertyOrder(int.MinValue + 4)][DsIndexedColumn] public int Sort { get; set; }
   
    /// <summary>
    /// Identifies the deletion state of the DsObject
    /// </summary>
    [JsonPropertyOrder(int.MinValue + 5)][DsIndexedColumn] public bool IsDeleted { get; set; }

    /// <summary>
    /// Object is not yet in the database
    /// </summary>
    [JsonIgnore]
    public bool IsNew { get; set; }

    /// <summary>
    /// Used in list views for column number
    /// </summary>
    [JsonIgnore]
    public long RowNum { get; set; }
    
    [JsonIgnore]
    public string Json
    {
        get
        {
            if (json.StringIsEmpty()) Serialize();

            return json;
        }
    }
    private string json = string.Empty;

    [JsonIgnore]
    private JsonDocument? jsonDocument;
    
    #endregion

    #region Json

    /// <summary>
    /// Load JSON data and establish a JsonDocument.
    /// </summary>
    /// <param name="dataStore"></param>
    public void LoadJson(string? jsonData)
    {
        json = jsonData ?? json;
        jsonDocument = JsonDocument.Parse(json);
    }
    
    /// <summary>
    /// Serialize the custom properties into the Json property.
    /// </summary>
    /// <param name="dataStore"></param>
    /// <param name="generateJsonDocument"></param>
    public void Serialize(DataStore? dataStore = null, bool generateJsonDocument = true)
    {
        var tableName = DataStore.GenerateTableName(GetType());
        var serializerContext = dataStore?.GetSerializerContext(tableName);

        json = string.Empty;

        if (serializerContext != null)
        {
            json = JsonSerializer.Serialize(this, GetType(), serializerContext);
        }

        if (json.StringIsEmpty())
        {
            json = JsonSerializer.Serialize(this, GetType(), DataStore.JsonSerializerOptions);
        }
        
        if (generateJsonDocument)
            jsonDocument = JsonDocument.Parse(json);
    }

    /// <summary>
    /// Deserialize JSON into this object.
    /// </summary>
    /// <param name="json"></param>
    /// <param name="dataStore"></param>
    public async Task Deserialize(string jsonText, DataStore? dataStore = null)
    {
        if (jsonText.StringHasValue())
        {
            var tableName = DataStore.GenerateTableName(GetType());
            var serializerContext = dataStore?.GetSerializerContext(tableName);
            object? newDso;

            if (serializerContext != null)
            {
                newDso = JsonSerializer.Deserialize(jsonText, GetType(), serializerContext);
            }

            else
            {
                newDso = JsonSerializer.Deserialize(jsonText, GetType(), DataStore.JsonSerializerOptions);
            }

            if (newDso != null)
            {
                await newDso.CloneObjectToAsync(this);
            }
        }
    }
    
    #endregion

    #region Typed Value Methods

    /// <summary>
    /// Get a single property value using a JSON path (dot notation).
    /// Used for value-type properties.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var feature1 = dso.Value(typeof(string), "Settings.Features.Feature1");
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="type"></param>
    /// <param name="propertyPath"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public object? Value(Type type, string propertyPath)
    {
        return Values(type, propertyPath)[0];
    }
    
    /// <summary>
    /// Get a list of property values using a JSON path (dot notation).
    /// Used for value-type properties.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var values = dso.Values(typeof(string), "Settings.Dictionary");
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="type"></param>
    /// <param name="propertyPath"></param>
    /// <returns></returns>
    public List<object?> Values(Type type, string propertyPath)
    {
        var results = new List<object?>();
        var internalTypes = typeof(DsObject).GetProperties().Select(p => p.Name);

        if (json.StringIsEmpty())
            Serialize();
        
        jsonDocument ??= JsonDocument.Parse(json);
        
        if (internalTypes.Contains(propertyPath.TrimStringStart("$.")))
        {
            results.Add(typeof(DsObject).GetProperty(propertyPath.TrimStringStart("$.") ?? string.Empty)?.GetValue(this));
        }

        else
        {
            if (propertyPath.StringHasValue())
            {
                var elements = JsonSelector.Select(jsonDocument.RootElement, propertyPath);

                if (elements.Any())
                {
                    foreach (var element in elements)
                    {
                        object? model = null;

                        if (type.IsSimpleDataType() == false)
                        {
                            model = element.Deserialize(type, DataStore.JsonSerializerOptions);
                        }
                        
                        if (model == null)
                        {
                            ProcessElement(type, element, results);
                        }

                        else
                        {
                            results.Add(model);
                        }
                    }
                }
            }
        }

        return results;
    }

    private static void ProcessElement(Type type, JsonElement element, ICollection<object?> results)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
            {
                foreach (var el in element.EnumerateObject())
                {
                    ProcessElement(type, el.Value, results);
                }

                break;
            }
            case JsonValueKind.Array:
            {
                foreach (var el in element.EnumerateArray())
                {
                    ProcessElement(type, el, results);
                }

                break;
            }
            case JsonValueKind.Undefined:
            case JsonValueKind.String:
            case JsonValueKind.Number:
            case JsonValueKind.True:
            case JsonValueKind.False:
            case JsonValueKind.Null:
            default:
                results.Add(GetElementValue(type, element));
                break;
        }
    }
    
    private static object? GetElementValue(Type type, JsonElement element)
    {
        if (type == typeof(byte))
        {
            if (element.ValueKind == JsonValueKind.Number)
                return element.GetByte();
        }

        else if (type == typeof(short))
        {
            if (element.ValueKind == JsonValueKind.Number)
                return element.GetInt16();
        }

        else if (type == typeof(int))
        {
            if (element.ValueKind == JsonValueKind.Number)
                return element.GetInt32();
        }

        else if (type == typeof(long))
        {
            if (element.ValueKind == JsonValueKind.Number)
                return element.GetInt64();
        }

        else if (type == typeof(double))
        {
            if (element.ValueKind == JsonValueKind.Number)
                return element.GetDouble();
        }

        else if (type == typeof(decimal))
        {
            if (element.ValueKind == JsonValueKind.Number)
                return element.GetDecimal();
        }

        else if (type == typeof(string))
        {
            if (element.ValueKind == JsonValueKind.String)
                return element.GetString() ?? string.Empty;
        }

        else if (type == typeof(bool))
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
            }
        }

        else if (type == typeof(DateOnly))
        {
            if (element.ValueKind == JsonValueKind.String)
                return DateOnly.FromDateTime(element.GetDateTime());
        }

        else if (type == typeof(TimeOnly))
        {
            if (element.ValueKind == JsonValueKind.String)
                return TimeOnly.FromDateTime(element.GetDateTime());
        }
        
        else if (type == typeof(DateTime))
        {
            if (element.ValueKind == JsonValueKind.String)
                return element.GetDateTime();
        }

        else if (type == typeof(DateTimeOffset))
        {
            if (element.ValueKind == JsonValueKind.String)
                return element.GetDateTimeOffset();
        }

        else if (type == typeof(Guid))
        {
            if (element.ValueKind == JsonValueKind.String)
                return element.GetGuid();
        }

        return Convert.ChangeType(element.GetString(), type);
    }
    
    #endregion
    
    #region Generic Value Methods
    
    /// <summary>
    /// Get a single property value using a JSON path (dot notation).
    /// Used for value-type properties.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var feature1 = dso.Value<string>("Settings.Features.Feature1");
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="propertyPath"></param>
    /// <returns></returns>
    public T? Value<T>(string propertyPath)
    {
        return Values<T>(propertyPath).Any() ? Values<T>(propertyPath)[0] : default;
    }

    /// <summary>
    /// Get a single property value using an expression.
    /// Used for value-type properties.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var email = dso.Value<User, string>(p => p.Profile.Email);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="expression"></param>
    /// <returns></returns>
    public U? Value<T,U>(Expression<Func<T, object?>> expression)
    {
        return Value<U>(GetJsonPropertyPath(expression));
    }
    
    /// <summary>
    /// Get a list of property values using a JSON path (dot notation).
    /// Used for value-type properties.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var terms = dso.Values<string>("Settings.TermsDictionary");
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="propertyPath"></param>
    /// <returns></returns>
    private List<T?> Values<T>(string propertyPath)
    {
        var results = Values(typeof(T), propertyPath);

        return results.Cast<T?>().ToList();
    }
    
    /// <summary>
    /// Get a list of property values using an expression.
    /// Used for value-type properties.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var terms = dso.Values<TestModel, string>(p => p.TermsDictionary);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="propertyPath"></param>
    /// <returns></returns>
    public List<U?> Values<T,U>(Expression<Func<T, object?>> expression)
    {
        return Values<U>(GetJsonPropertyPath(expression));
    }

    /// <summary>
    /// Get a list of property values from nested objects using an expression.
    /// Used for value-type properties.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var emails = dso.Values<User, Email, string>(p => p.Profile, e => e.Email);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="propertyPath"></param>
    /// <returns></returns>
    public List<V?> Values<T,U,V>(Expression<Func<T, object?>> expressionPrefix, Expression<Func<U, object?>> expression)
    {
        return Values<V>(GetJsonPropertyPath(expressionPrefix, expression));
    }

    public List<W?> Values<T,U,V,W>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expression)
    {
        return Values<W>(GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expression));
    }

    public List<X?> Values<T,U,V,W,X>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expression)
    {
        return Values<X>(GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expression));
    }

    public List<Y?> Values<T,U,V,W,X,Y>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expression)
    {
        return Values<Y>(GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expression));
    }
    
    public List<Z?> Values<T,U,V,W,X,Y,Z>(Expression<Func<T, object?>> expressionPrefix1, Expression<Func<U, object?>> expressionPrefix2, Expression<Func<V, object?>> expressionPrefix3, Expression<Func<W, object?>> expressionPrefix4, Expression<Func<X, object?>> expressionPrefix5, Expression<Func<Y, object?>> expression)
    {
        return Values<Z>(GetJsonPropertyPath(expressionPrefix1, expressionPrefix2, expressionPrefix3, expressionPrefix4, expressionPrefix5, expression));
    }
    
    #endregion
    
    #region GetJsonPropertyPath()

    public static string _GetFullPropertyPath<T>(Expression<Func<T, object?>> expression, string pathPrefix = "", bool forJsonPath = false)
    {
        var propName = string.Empty;

        switch (expression.Body)
        {
            case UnaryExpression ue:
            {
                #region Handle property converters
            
                var exp = ue.Operand;

                while (exp is {NodeType: ExpressionType.MemberAccess})
                {
                    exp = (exp as MemberExpression)?.Expression;
                }

                if (exp?.NodeType == ExpressionType.Parameter)
                {
                    var paramName = expression.Parameters[0].Name;
                    propName = ue.Operand.ToString().Replace($"{paramName}.", string.Empty);
                }

                break;
                
                #endregion
            }
        }
        
        #region Handle standard JSON properties
        
        if (propName.StringIsEmpty())
        {
            var name = expression.Parameters[0].Name;
            propName = expression.ToString().Replace($"{name} => {name}.", string.Empty);

            if (propName.Contains("get_Item(", StringComparison.InvariantCultureIgnoreCase))
            {
                propName = propName.Replace("get_Item(\"", string.Empty).Replace("\")", string.Empty, StringComparison.InvariantCultureIgnoreCase);
                propName = propName.Replace("get_Item(", string.Empty).Replace(")", string.Empty, StringComparison.InvariantCultureIgnoreCase);
            }
        }
        
        #endregion

        var result = $"{(pathPrefix.StringHasValue() ? pathPrefix.TrimEnd('.') + "." : "")}{propName}";

        if (forJsonPath && expression.Body.Type.IsSimpleDataType() == false)
        {
            if (Activator.CreateInstance(expression.Body.Type) is IList || Activator.CreateInstance(expression.Body.Type) is Array)
            {
                result += "[*]";
            }
        }

        return result;
    }
    
    public static string GetJsonPropertyPath<T>(Expression<Func<T, object?>> expression)
    {
        return $"$.{_GetFullPropertyPath(expression, forJsonPath: true)}";
    }

    public static string GetJsonPropertyPath<T,U>(Expression<Func<T, object?>> expressionSegment1, Expression<Func<U, object?>> expression)
    {
        return $"$.{_GetFullPropertyPath(expression, _GetFullPropertyPath(expressionSegment1, forJsonPath: true), forJsonPath: true)}";
    }
    
    public static string GetJsonPropertyPath<T,U,V>(Expression<Func<T, object?>> expressionSegment1, Expression<Func<U, object?>> expressionSegment2, Expression<Func<V, object?>> expression)
    {
        return $"$.{_GetFullPropertyPath(expression, _GetFullPropertyPath(expressionSegment2, _GetFullPropertyPath(expressionSegment1, forJsonPath: true), forJsonPath: true), forJsonPath: true)}";
    }

    public static string GetJsonPropertyPath<T,U,V,W>(Expression<Func<T, object?>> expressionSegment1, Expression<Func<U, object?>> expressionSegment2, Expression<Func<V, object?>> expressionSegment3, Expression<Func<W, object?>> expression)
    {
        return $"$.{_GetFullPropertyPath(expression, _GetFullPropertyPath(expressionSegment3, _GetFullPropertyPath(expressionSegment2, _GetFullPropertyPath(expressionSegment1, forJsonPath: true), forJsonPath: true), forJsonPath: true), forJsonPath: true)}";
    }

    public static string GetJsonPropertyPath<T,U,V,W,X>(Expression<Func<T, object?>> expressionSegment1, Expression<Func<U, object?>> expressionSegment2, Expression<Func<V, object?>> expressionSegment3, Expression<Func<W, object?>> expressionSegment4, Expression<Func<X, object?>> expression)
    {
        return $"$.{_GetFullPropertyPath(expression, _GetFullPropertyPath(expressionSegment4, _GetFullPropertyPath(expressionSegment3, _GetFullPropertyPath(expressionSegment2, _GetFullPropertyPath(expressionSegment1, forJsonPath: true), forJsonPath: true), forJsonPath: true), forJsonPath: true), forJsonPath: true)}";
    }

    public static string GetJsonPropertyPath<T,U,V,W,X,Y>(Expression<Func<T, object?>> expressionSegment1, Expression<Func<U, object?>> expressionSegment2, Expression<Func<V, object?>> expressionSegment3, Expression<Func<W, object?>> expressionSegment4, Expression<Func<X, object?>> expressionSegment5, Expression<Func<Y, object?>> expression)
    {
        return $"$.{_GetFullPropertyPath(expression, _GetFullPropertyPath(expressionSegment5, _GetFullPropertyPath(expressionSegment4, _GetFullPropertyPath(expressionSegment3, _GetFullPropertyPath(expressionSegment2, _GetFullPropertyPath(expressionSegment1, forJsonPath: true), forJsonPath: true), forJsonPath: true), forJsonPath: true), forJsonPath: true), forJsonPath: true)}";
    }

    #endregion

    // To detect redundant calls
    private bool _disposedValue;

    ~DsObject() => Dispose(false);
    
    // Public implementation of Dispose pattern callable by consumers.
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
   
    // Protected implementation of Dispose pattern.
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                jsonDocument?.Dispose();
            }

            _disposedValue = true;
        }
    }
}