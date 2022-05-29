# DataStore

DataStore is a high performance JSON object store (ORM) for SQL Server. DataStore uses and automatically creates and manages a pre-defined SQL Server data structure that can coexist with existing database objects. All database operations are performed with the DataStore class.  

Your models are stored as JSON so you can have most any kind of object structure, provided your models inherit from DsObject.

## Basic Example

Instantiating DataStore with settings is non-destructive. Any existing DataStore tables are left untouched. Methods to delete all or unused schema objects are provided for those edge cases.  

### Models

Instantiate DataStore with a settings object and database schema will be created for all classes that inherit from DsObject. The following attributes can be used in your classes:  

- *DsNoDatabaseTable* prevents DataStore from creating a table for the class.
- *DsUseLineageFeatures* enables lineage features for that table; add to the class itself.
- *DsSerializerContext(typeof(...))* to provide a de/serialization speed boost by using source generator *JsonSerializationContext* classes for each table; add to the class itself.
- *DsIndexedColumn* generates a SQL computed column with index for faster queries on that data; add to properties and fields.
- *DsIndexedColumn("Food","Email")* generates indexed SQL computed columns for faster queries on the dictionary key names specified; add to Dictionary properties and fields.

```C#
[DsUseLineageFeatures]
[DsSerializerContext(typeof(UserJsonSerializerContext))]
public class User: DsObject
{
    [DsIndexedColumn]
    public string Firstname { get; set; }
    
    [DsIndexedColumn]
    public int Age { get; set; }
    
    public List<Permissions> Permissions { get; set; }
    
    [DsIndexedColumn("Food", "Color")]
    public Dictionary<string, string> Favorites { get; set; } = new();
}

[JsonSerializable(typeof(User))]
[JsonSourceGenerationOptions(WriteIndented = false)]
internal partial class UserJsonSerializerContext : JsonSerializerContext
{ }
```

### Construction

You can create a DataStore instance anywhere in your code:

```C#
var dataStore = new DataStore(new DataStoreSettings {
    SqlConnectionString = sqlConnectionString,
    UseIndexedColumns = true
});
```

You can also use DataStore as a singleton service:

```C#
services.AddSingleton<DataStore>((factory) => new DataStore(new DataStoreSettings {
    SqlConnectionString = sqlConnectionString,
    UseIndexedColumns = true
}));
```

### Create and Save Objects

Creating and saving a DataStore object is simple:

```C#
var user = new User
{
    FirstName = "Michael",
    LastName = "Argentini",
    Age = 50,
    Permissions = new List<Permission>
    {
        new() { Role = "user" },
        new() { Role = "admin" },
        // etc.
    }
};

await dataStore.SaveAsync(user);
```

The saved object is updated with any changes, like lineage and depth information, creation or last update date, etc. And you can provide a list of objects to save them all in one call.  

### Read Objects

Querying the database for objects is simple too. In any read calls you can specify a DsQuery object with a fluent-style pattern for building your query. In the query you can specify property names as strings with dot notation:

```C#
var users = await dataStore.GetManyAsync<User>(
    page: 1,
    perPage: 50,
    new DsQuery("User")
        .StringProp("LastName").EqualTo("Argentini")
        .AND()
        .StringProp("Permissions.Role").EqualTo("admin")
        .AND()
        .GroupBegin()
            .NumberProp<int>("Age").EqualTo(50)
            .OR()
            .NumberProp<int>("Age").EqualTo(51)
        .GroupEnd(),
    new DsOrderBy()
        .Prop<int>("Age").Ascending()
);
```

Or you can use the model structure to specify names, and make code refactoring easier:

```C#
var users = await dataStore.GetManyAsync<User>(
    page: 1,
    perPage: 50,
    new DsQuery("User")
        .StringProp<User>(u => u.LastName).EqualTo("Argentini")
        .AND()
        .StringProp<User, Role>(u => u.Permissions, r => r.Role).EqualTo("admin")
        .AND()
        .GroupBegin()
            .NumberProp<User,int>(u => u.Age).EqualTo(50)
            .OR()
            .NumberProp<User,int>(u => u.Age).EqualTo(51)
        .GroupEnd(),
    new DsOrderBy()
        .Prop<User>(o => o.Age).Ascending()
);
```

### Dynamic Object Access

If you need to access object properties without knowing the object type, DsObject exposes JSON features that allow you to access property values using standard JSON path syntax:

```C#
var users = await dataStore.GetManyAsync<User>(
    page: 1,
    perPage: 50
);

foreach (DsObject dso in users)
{
    dso.Serialize(dataStore);

    var lastName = dso.Value<string>("$.LastName");
    var roles = dso.Values(typeof(string), "$.Permissions.Role");

    // etc.
}

```

**Remember:** these JSON features are read-only. If you change a property value in the DsObject you will need to call *Serialize()* again to update the JSON representation.


### Project

This project is a .NET 6.0 library with xUnit tests, so you can easily play with DataStore. These tests show how to use DataStore as well as benchmark its performance.  
