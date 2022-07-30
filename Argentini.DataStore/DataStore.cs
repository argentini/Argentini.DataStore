using System.Collections;
using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.ObjectPool;

namespace Argentini.DataStore;

/// <summary>
/// DataStore is a high performance JSON object store for SQL Server.
/// DataStore uses and automatically creates and manages a pre-defined SQL Server data structure that can coexist with existing database objects.
/// All database operations are performed with the DataStore class.
///
/// Objects are stored as serialized JSON so you can have most any kind of object structure, provided your models inherit from DsObject.
/// </summary>
/// <example>
/// Instantiating DataStore with settings is non-destructive. Any existing DataStore tables are left untouched.
/// Methods to delete all or unused schema objects are provided.
///
/// Instantiate DataStore with a settings object and database schema will be created for all classes that inherit from DsObject. The following attributes can be used in your classes:  
///
/// [DsUseLineageFeatures] enables lineage features for that table; add to the class itself
/// [DsSerializerContext(typeof(...))] to provide a de/serialization speed boost by using source generator *JsonSerializationContext* classes for each table; add to the class itself
/// [DsIndexedColumn] generates a SQL computed column with index for faster queries on that data; add to properties and fields
/// [DsIndexedColumn("Food","Email")] generates indexed SQL computed columns for faster queries on the dictionary key names specified; add to Dictionary properties and fields
/// <code>
/// <![CDATA[
/// var dataStore = new DataStore(new DataStoreSettings {
///     SqlConnectionString = sqlConnectionString,
///     UseIndexedColumns = true
///     });
/// ]]>
/// </code>
/// </example>
/// <example>
/// You can also use DataStore as a singleton service.
/// <code>
/// <![CDATA[
/// services.AddSingleton<DataStore>((factory) => new DataStore(new DataStoreSettings {
///     SqlConnectionString = sqlConnectionString,
///     UseIndexedColumns = true
///     }));
/// ]]>
/// </code>
/// </example>
/// <example>
/// Creating and saving a DataStore object is simple.
/// <code>
/// <![CDATA[
/// [DsUseLineageFeatures]
/// [DsSerializerContext(typeof(UserJsonSerializerContext))]
/// public class User: DsObject
/// {
///     [DsIndexedColumn] public string Firstname { get; set; }
///     [DsIndexedColumn] public int Age { get; set; }
///     public List<Permissions> Permissions { get; set; }
///     ...
/// }
/// 
/// var user = new User
/// {
///     Firstname = "Michael",
///     ...
/// };
///
/// await dataStore.SaveAsync(user);
/// ]]>
/// </code>
/// </example>
/// <example>
/// Querying the database for objects is simple too. You can specify property names as strings with dot notation:
/// <code>
/// <![CDATA[
/// var users = await dataStore.GetManyAsync<User>(
///     page: 1, perPage: 50,
///     new DsQuery("Manager")
///         .StringProp("Name.LastName").EqualTo("Argentini")
///         .AND()
///         .NumberProp<int>("Age").GreaterThan(49)
///         .AND()
///         .GroupBegin()
///             .NumberProp<int>("Age").EqualTo(51)
///             .OR()
///             .NumberProp<int>("Age").EqualTo(52)
///         .GroupEnd(),
///     new DsOrderBy()
///         .Prop<int>("Age").Ascending()
///     );
/// ]]>
/// </code>
/// </example>
/// <example>
/// And you can query using expressions:
/// <code>
/// <![CDATA[
/// var users = await dataStore.GetManyAsync<User>(
///     page: 1, perPage: 50,
///     new DsQuery("Manager")
///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
///         .AND()
///         .GroupBegin()
///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
///             .OR()
///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
///         .GroupEnd(),
///     new DsOrderBy()
///         .DepthAscending()
///         .Prop<User,int>(p => p.Age).Ascending()
///     );
/// ]]>
/// </code>
/// </example>
public class DataStore
{
    #region Constants

    /// <summary>
    /// Used for determining the type of numeric value to increment, etc. 
    /// </summary>
    // ReSharper disable once MemberCanBePrivate.Global
    public enum NumericTypes
    {
        Decimal,
        Double,
        Integer,
        Long
    }

    /// <summary>
    /// Used for determining the type of non-numeric value to update, etc. 
    /// </summary>
    // ReSharper disable once MemberCanBePrivate.Global
    public enum AllTypes
    {
        Bool,
        DateTime,
        Decimal,
        Double,
        Guid,
        Integer,
        Long,
        Text
    }
    
    /// <summary>
    /// Used to specify the date increment 
    /// </summary>
    // ReSharper disable once MemberCanBePrivate.Global
    public enum DateTimeIncrements
    {
        Year,
        Quarter,
        Month,
        DayOfYear,
        Day,
        Week,
        Weekday,
        Hour,
        Minute,
        Second,
        Millisecond
    }

    public const double Version = 3.3d;
    // ReSharper disable once MemberCanBePrivate.Global
    public const int MinSqlMajorVersionSupported = 13;
    // ReSharper disable once MemberCanBePrivate.Global
    public const int MinSqlMinorVersionSupported = 0;
    // ReSharper disable once MemberCanBePrivate.Global
    public const int MinSqlBuildVersionSupported = 4001;
    // ReSharper disable once MemberCanBePrivate.Global
    public const string MinSqlServerVersion = "SQL Server 2016 SP1";

    #endregion
    
    #region Constructors

    /// <summary>
    /// Instantiate a new DataStore object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var dataStore = new DataStore(new DataStoreSettings {
    ///     SqlConnectionString = sqlConnectionString
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="settings"></param>
    public DataStore(DataStoreSettings settings)
    {
        Init(settings);
    }

    /// <summary>
    /// Instantiate a new DataStore object.
    /// Must be initialized before use using Init().
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var dataStore = new DataStore();
    /// ]]>
    /// </code>
    /// </example>
    public DataStore()
    {
    }

    #endregion
    
    #region Init
    
    private static void TableNameIsValid(DataStoreTableSetting setting)
    {
        var tableName = setting.TableName;
        var r = new Regex(@"^[A-Za-z_0-9]+$");

        if (tableName.StringIsEmpty())
        {
            throw new Exception("DataStore() => Custom table name cannot be empty");
        }

        if (r.IsMatch(tableName) == false)
        {
            throw new Exception($"DataStore() => Custom table name can only contain letters, numbers, and underscore: {tableName}");
        }
    }
    
    /// <summary>
    /// Initialize the data store; only call this once!
    /// </summary>
    /// <param name="settings"></param>
    /// <exception cref="Exception"></exception>
    // ReSharper disable once MemberCanBePrivate.Global
    public void Init(DataStoreSettings settings)
    {
        var modelTypes = new List<Type>();

        Settings = settings;

        foreach (var modelType in Objects.GetInheritedTypes(typeof(DsObject)).ToList())
        {
            var noTable = false;
            
            foreach (var attr in modelType.GetCustomAttributes(true).ToList())
            {
                var custAttr = attr is DsNoDatabaseTable attr1 ? attr1 : default;

                if (custAttr != null)
                {
                    noTable = true;
                }
            }
            
            if (noTable == false)
                modelTypes.Add(modelType);
        }

        if (modelTypes.Count > 0)
        {
            TableDefinitions.Clear();
            
            #region Gather Table Definitions

            foreach (var type in modelTypes)
            {
                var tableDef = new DataStoreTableSetting
                {
                    TableName = GenerateTableName(type)
                };

                foreach (var attr in type.GetCustomAttributes(true).ToList())
                {
                    var custAttr = attr is DsUseLineageFeatures attr1 ? attr1 : default;

                    if (custAttr != null)
                    {
                        tableDef.UseLineageFeatures = true;
                    }

                    var custAttr2 = attr is DsSerializerContext attr2 ? attr2 : default;

                    if (custAttr2 is {Values.Length: > 0})
                    {
                        var context = custAttr2.Values[0].GetProperty("Default")?.GetValue(null, null);

                        if (context != null)
                        {
                            tableDef.ModelSerializerContext = (JsonSerializerContext) context;
                        }
                    }
                }

                TableDefinitions.Add(tableDef);
            }

            #endregion

            if (Settings.SqlConnectionString.StringHasValue())
            {
                #region SQL Server Version Check

                var sqlMajorVersion = 0;
                var sqlMinorVersion = 0;
                var sqlBuildVersion = 0;
                var sqlServerSupported = false;
                var timer = new Stopwatch();

                using (var cn = new SqlConnection())
                {
                    cn.ConnectionString = Settings.SqlConnectionString;
                    timer.Start();

                    var task = cn.OpenAsync();

                    while (task.IsCompleted == false && timer.Elapsed.Seconds <= 15)
                    {
                        Thread.Sleep(100);
                    }

                    if (cn.State == ConnectionState.Open && cn.ServerVersion != null)
                    {
                        var versionSplits = cn.ServerVersion.Split('.');

                        if (versionSplits.Length > 0)
#pragma warning disable CA1806
                            int.TryParse(cn.ServerVersion.Split('.')[0], out sqlMajorVersion);
#pragma warning restore CA1806

                        if (versionSplits.Length > 1)
#pragma warning disable CA1806
                            int.TryParse(cn.ServerVersion.Split('.')[1], out sqlMinorVersion);
#pragma warning restore CA1806

                        if (versionSplits.Length > 2)
#pragma warning disable CA1806
                            int.TryParse(cn.ServerVersion.Split('.')[2], out sqlBuildVersion);
#pragma warning restore CA1806

                        SqlServerVersion = sqlMajorVersion + "." + sqlMinorVersion + "." + sqlBuildVersion;

                        switch (sqlMajorVersion)
                        {
                            case > MinSqlMajorVersionSupported:
                                sqlServerSupported = true;
                                break;
                            case MinSqlMajorVersionSupported:
                            {
                                switch (sqlMinorVersion)
                                {
                                    case > MinSqlMinorVersionSupported:
                                        sqlServerSupported = true;
                                        break;
                                    case MinSqlMinorVersionSupported:
                                    {
                                        if (sqlBuildVersion >= MinSqlBuildVersionSupported)
                                        {
                                            sqlServerSupported = true;
                                        }

                                        break;
                                    }
                                }

                                break;
                            }
                        }
                    }

                    else
                    {
                        throw new Exception(
                            "DataStore() => Could not connect to the SQL Server (connection timeout)");
                    }

                    if (sqlServerSupported == false)
                    {
                        throw new Exception(
                            $"DataStore() => Unsupported SQL Server version; minimum supported version is {MinSqlServerVersion} ({MinSqlMajorVersionSupported}.{MinSqlMinorVersionSupported}.{MinSqlBuildVersionSupported}), this server is version {sqlMajorVersion}.{sqlMinorVersion}.{sqlBuildVersion}");
                    }
                }

                #endregion

                #region Check and Create Tables, Run Migrations

                var migration = new DsMigration(this);
                
                var create = string.Empty;
                var freshBuild = true;
                var tableNames = new List<string>();

                if (TableDefinitions.Any())
                {
                    foreach (var definition in TableDefinitions)
                    {
                        TableNameIsValid(definition);
                    }

                    tableNames.AddRange(TableDefinitions.Select(d => d.TableName).ToList());
                }

                using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
                {
                    using (var sqlCmd = new SqlCommand())
                    {
                        try
                        {
                            sqlCmd.CommandText = @"SELECT TOP 1 [name] FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[datastore]') AND OBJECTPROPERTY(id, N'IsTable') = 1";
                            sqlCmd.Connection = sqlConnection;
                            sqlConnection.Open();

                            if (sqlConnection.State == ConnectionState.Open)
                            {
                                using (var sqlDataReader = sqlCmd.ExecuteReader())
                                {
                                    if (sqlDataReader.HasRows)
                                    {
                                        freshBuild = false;
                                    }
                                }
                            }

                            else
                            {
                                throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                            }
                        }

                        catch (Exception e)
                        {
                            throw new Exception($"SqlConnection() => {e.Message}");
                        }
                    }
                }

                if (freshBuild == false)
                {
                    migration.RunPreMigrations();
                }
                
                bool dataReady;
                
                foreach (var tableName in tableNames)
                {
                    dataReady = false;
                    
                    using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
                    {
                        using (var sqlCmd = new SqlCommand())
                        {
                            try
                            {
                                sqlCmd.CommandText = $@"SELECT TOP 1 [name] FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[datastore_{tableName}]') AND OBJECTPROPERTY(id, N'IsTable') = 1";
                                sqlCmd.Connection = sqlConnection;
                                sqlConnection.Open();

                                if (sqlConnection.State == ConnectionState.Open)
                                {
                                    using (var sqlDataReader = sqlCmd.ExecuteReader())
                                    {
                                        if (sqlDataReader.HasRows)
                                        {
                                            dataReady = true;
                                        }
                                    }
                                }

                                else
                                {
                                    throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                                }
                            }

                            catch (Exception e)
                            {
                                throw new Exception($"SqlConnection() => {e.Message}");
                            }
                        }
                    }
                    
                    if (dataReady == false)
                    {
                        create += $@"
create table [dbo].[datastore_{tableName}] (
[json_data] nvarchar(max) not null
)

alter table [dbo].[datastore_{tableName}] add constraint [DF_{tableName}_json_is_valid] CHECK (ISJSON(json_data) > 0)
";
                    }
                }

                dataReady = false;

                using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
                {
                    using (var sqlCmd = new SqlCommand())
                    {
                        try
                        {
                            sqlCmd.CommandText = @"SELECT * FROM dbo.sysobjects WHERE id = object_id(N'[dbo].[datastore]') AND OBJECTPROPERTY(id, N'IsTable') = 1";
                            sqlCmd.Connection = sqlConnection;
                            sqlConnection.Open();

                            if (sqlConnection.State == ConnectionState.Open)
                            {
                                using (var sqlDataReader = sqlCmd.ExecuteReader())
                                {
                                    if (sqlDataReader.HasRows)
                                    {
                                        dataReady = true;
                                    }
                                }
                            }

                            else
                            {
                                throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                            }
                        }

                        catch (Exception e)
                        {
                            throw new Exception($"SqlConnection() => {e.Message}");
                        }
                    }
                }
                
                if (dataReady == false)
                {
                    create += $@"
CREATE TABLE [dbo].[datastore](
[version] [float] NOT NULL
)

INSERT INTO [dbo].[datastore] ([version]) VALUES ({(freshBuild ? Version : "2.0")})
";
                }

                if (create.StringHasValue())
                {
                    try
                    {
                        _ = new SqlExec(new SqlExecSettings
                        {
                            SqlConnectionString = Settings.SqlConnectionString,
                            CommandString = create
                        });
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlExec() => {e.Message}");
                    }
                }

                if (freshBuild == false)
                {
                    migration.RunMigrations();
                }

                #endregion

                #region Create Computed Columns and Indexes

                IndexedColumns.Clear();

                foreach (var type in modelTypes)
                {
                    ExplorePropertiesAndFields(type);
                }

                var newIndexedColumns = new List<DataStoreIndexedColumn>(IndexedColumns);
                var clusteredIndexes = new List<string>();
                var changeScript = StringBuilderPool.Get();

                #region Ensure each table (model) has only one clustered index; use first
                
                foreach (var column in newIndexedColumns)
                {
                    if (column.IsClustered)
                    {
                        if (clusteredIndexes.Contains(column.ModelType.Name))
                        {
                            column.IsClustered = false;
                        }

                        else
                        {
                            clusteredIndexes.Add(column.ModelType.Name);
                        }
                    }
                }
                
                #endregion
                
                foreach (var modelType in modelTypes)
                {
                    var computedColumns = $@"
SELECT
    columns.name AS [COLUMN_NAME],
	cc.definition AS [COMPUTED_VALUE]
FROM sys.columns columns
INNER JOIN (
	SELECT
		t.object_id,
		s.Name AS [TABLE_SCHEMA],
		t.NAME AS [TABLE_NAME]
	FROM
		sys.tables t
    INNER JOIN
		sys.schemas s ON t.schema_id = s.schema_id
) tables ON tables.object_id = columns.object_id
LEFT OUTER JOIN (
	SELECT *
	FROM SYS.computed_columns
) cc ON cc.object_id = tables.object_id AND cc.column_id = columns.column_id
WHERE
    object_name(columns.object_id) = 'datastore_{GenerateTableName(modelType)}'
    AND tables.TABLE_SCHEMA = 'dbo'
    AND cc.definition IS NOT NULL
";

                    using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
                    {
                        using (var sqlCmd = new SqlCommand())
                        {
                            try
                            {
                                sqlCmd.CommandText = computedColumns;
                                sqlCmd.Connection = sqlConnection;
                                sqlConnection.Open();

                                if (sqlConnection.State == ConnectionState.Open)
                                {
                                    using (var sqlDataReader = sqlCmd.ExecuteReader())
                                    {
                                        if (sqlDataReader.HasRows)
                                        {
                                            while (sqlDataReader.Read())
                                            {
                                                var ccName = sqlDataReader.SqlSafeGetString("COLUMN_NAME");
                                                var cc = IndexedColumns.FirstOrDefault(p => p.ModelType == modelType && p.ColumnName == ccName);

                                                if (cc == null)
                                                {
                                                    changeScript.Append($@"
DROP INDEX IF EXISTS ix_{ccName} ON [dbo].[datastore_{GenerateTableName(modelType)}]
ALTER TABLE [dbo].[datastore_{GenerateTableName(modelType)}] DROP COLUMN [{ccName}]
");
                                                }

                                                else
                                                {
                                                    var existingCC = sqlDataReader.SqlSafeGetString("COMPUTED_VALUE");

                                                    if (existingCC.Contains(cc.Crc))
                                                    {
                                                        newIndexedColumns.Remove(cc);
                                                    }

                                                    else
                                                    {
                                                        changeScript.Append($@"
DROP INDEX IF EXISTS ix_{ccName} ON [dbo].[datastore_{GenerateTableName(modelType)}]
ALTER TABLE [dbo].[datastore_{GenerateTableName(modelType)}] DROP COLUMN [{ccName}]
");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                else
                                {
                                    throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                                }
                            }

                            catch (Exception e)
                            {
                                throw new Exception($"SqlConnection() => {e.Message}");
                            }
                        }
                    }
                }

                foreach (var cc in newIndexedColumns)
                {
                    changeScript.Append($@"
ALTER TABLE [dbo].[datastore_{GenerateTableName(cc.ModelType)}] ADD [{cc.ColumnName}] AS {cc.ComputedValue}
CREATE{(cc.IsUnique ? " UNIQUE" : string.Empty)}{(cc.IsClustered ? " CLUSTERED" : " NONCLUSTERED")} INDEX ix_{cc.ColumnName} ON [dbo].[datastore_{GenerateTableName(cc.ModelType)}]([{cc.ColumnName}])
");
                }

                if (changeScript.SbHasValue())
                {
                    try
                    {
                        _ = new SqlExec(new SqlExecSettings
                        {
                            SqlConnectionString = Settings.SqlConnectionString,
                            CommandString = changeScript.ToString()
                        });
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlExec() => {e.Message}");
                    }
                }
                
                StringBuilderPool.Return(changeScript);

                #endregion
                
                #region Check and Create Functions

                 foreach (var tableName in tableNames)
                 {
                    create = $@"
CREATE OR ALTER FUNCTION [dbo].[udf_datastore_{tableName}_GetDepth]
(
@id uniqueidentifier = NULL
)
RETURNS int
AS
BEGIN

DECLARE @depth int;

;WITH ancestors AS (
	SELECT [Id], [ParentId]
	FROM dbo.[datastore_{tableName}]
	WHERE [Id] = @id
	UNION ALL
	SELECT c.[Id], c.[ParentId]
	FROM dbo.[datastore_{tableName}] c
	JOIN ancestors p ON p.[ParentId] = c.[Id]
) 
SELECT @depth = COUNT(*)
FROM ancestors
WHERE [Id] <> @id;

RETURN @depth
END
";
                    try
                    {
                        _ = new SqlExec(new SqlExecSettings
                        {
                            SqlConnectionString = Settings.SqlConnectionString,
                            CommandString = create
                        });
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlExec() => Failed to create [dbo].[udf_datastore_{tableName}_GetDepth]; {e.Message}");
                    }

                    create = $@"
CREATE OR ALTER FUNCTION [dbo].[udf_datastore_{tableName}_GetLineage]
(
@id uniqueidentifier = NULL
)
RETURNS nvarchar(MAX)
AS
BEGIN

DECLARE @lineage nvarchar(MAX);

;WITH ancestors AS (
	SELECT [Id], [ParentId]
	FROM dbo.[datastore_{tableName}]
	WHERE [Id] = @id
	UNION ALL
	SELECT c.[Id], c.[ParentId]
	FROM dbo.[datastore_{tableName}] c
	JOIN ancestors p ON p.[ParentId] = c.[Id]
) 
SELECT @lineage = '[""' + STRING_AGG(CONVERT(nvarchar(MAX), [Id]), '"",""') + '""]'
FROM ancestors
WHERE [Id] <> @id;

IF @lineage = '[""""]' BEGIN
    SET @lineage = NULL
END

RETURN @lineage
END
";

                    try
                    {
                        _ = new SqlExec(new SqlExecSettings
                        {
                            SqlConnectionString = Settings.SqlConnectionString,
                            CommandString = create
                        });
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlExec() => Failed to create [dbo].[udf_datastore_{tableName}_GetLineage]; {e.Message}");
                    }

                    create = $@"
CREATE OR ALTER FUNCTION [dbo].[udf_datastore_{tableName}_HasAncestor]
(
@id uniqueidentifier = NULL
,@ancestorId uniqueidentifier = NULL
)
RETURNS int
AS
BEGIN

DECLARE @match uniqueidentifier = NULL
DECLARE @result int = 0

;WITH ancestors AS (
	SELECT [Id], [ParentId]
	FROM dbo.[datastore_{tableName}]
	WHERE [Id] = @id
	UNION ALL
	SELECT c.[Id], c.[ParentId]
	FROM dbo.[datastore_{tableName}] c
	JOIN ancestors p ON p.[ParentId] = c.[Id]
) 
SELECT @match = [Id]
FROM ancestors
WHERE [Id] = @ancestorId;

IF @match IS NOT NULL AND @match = @ancestorId
BEGIN
    SET @result = 1
END

RETURN @result
END
";

                    try
                    {
                        _ = new SqlExec(new SqlExecSettings
                        {
                            SqlConnectionString = Settings.SqlConnectionString,
                            CommandString = create
                        });
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlExec() => Failed to create [dbo].[udf_datastore_{tableName}_HasAncestor]; {e.Message}");
                    }
                }

                #endregion

                #region Check and Create Stored Procedures

                foreach (var tableName in tableNames)
                {
                    create = $@"
CREATE OR ALTER PROCEDURE [dbo].[usp_datastore_{tableName}_UpdateDescendantLineages]
@parent_object_id uniqueidentifier = NULL
AS
BEGIN
BEGIN TRAN
    BEGIN TRY

        DECLARE @RC INT

        EXEC @RC = sp_getapplock
                @Resource = 'StoredProcUsingAppLock',
                @LockMode = 'Exclusive',
                @LockOwner = 'Transaction',
                @LockTimeout = 60000 -- 1 minute

        IF @RC < 0
        BEGIN

            IF @@TRANCOUNT > 0 ROLLBACK TRAN

        END ELSE BEGIN

			DECLARE @Descendants TABLE (rownum int IDENTITY (1, 1) PRIMARY KEY NOT NULL, descendant_id uniqueidentifier)
			DECLARE @RowCnt int
			DECLARE @MaxRows int
			DECLARE @descendant_id uniqueidentifier
			DECLARE @json nvarchar(max)
			DECLARE @lineage nvarchar(max)
			DECLARE @depth int

            IF @parent_object_id IS NOT NULL BEGIN

				;WITH descendants AS (
					SELECT  [Id],
							[ParentId],
							[Depth]
					FROM [dbo].[datastore_{tableName}]
					WHERE [Id] = @parent_object_id

					UNION ALL

					SELECT  ft.[Id],
							ft.[ParentId],
							ft.[Depth]
					FROM [dbo].[datastore_{tableName}] ft
					JOIN descendants d
					ON ft.[ParentId] = d.[Id]
				)

				INSERT INTO @Descendants (descendant_id)
	            SELECT [Id]
	            FROM descendants

				SET @RowCnt = 1
				SET @MaxRows = (SELECT COUNT(*) FROM @Descendants)

				WHILE @RowCnt <= @MaxRows
				BEGIN
					SET @descendant_id = (SELECT [descendant_id] FROM @Descendants WHERE [rownum] = @RowCnt)
                    SET @lineage = dbo.udf_datastore_{tableName}_GetLineage(@descendant_id)
                    SET @depth = dbo.udf_datastore_{tableName}_GetDepth(@descendant_id)

                    SET @json = (SELECT TOP 1 [json_data] FROM [dbo].[datastore_{tableName}] WHERE [Id] = @descendant_id)
                    SET @json = JSON_MODIFY(JSON_MODIFY(@json,'$.Lineage',JSON_Query(@lineage)), '$.Depth', @depth)

                    UPDATE [dbo].[datastore_{tableName}]
                    SET
                        [json_data] = @json
                    WHERE [Id] = @descendant_id

					SET @RowCnt = @RowCnt + 1
				END

            END
			ELSE BEGIN

				INSERT INTO @Descendants (descendant_id)
	            SELECT [Id]
	            FROM [dbo].[datastore_{tableName}]

				SET @RowCnt = 1
				SET @MaxRows = (SELECT COUNT(*) FROM @Descendants)

				WHILE @RowCnt <= @MaxRows
				BEGIN
					SET @descendant_id = (SELECT [descendant_id] FROM @Descendants WHERE [rownum] = @RowCnt)
                    SET @lineage = dbo.udf_datastore_{tableName}_GetLineage(@descendant_id)
                    SET @depth = dbo.udf_datastore_{tableName}_GetDepth(@descendant_id)

                    SET @json = (SELECT TOP 1 [json_data] FROM [dbo].[datastore_{tableName}] WHERE [Id] = @descendant_id)
                    SET @json = JSON_MODIFY(JSON_MODIFY(@json,'$.Lineage',JSON_Query(@lineage)), '$.Depth', @depth)

                    UPDATE [dbo].[datastore_{tableName}]
                    SET
                        [json_data] = @json
                    WHERE [Id] = @descendant_id

					SET @RowCnt = @RowCnt + 1
				END

			END

            COMMIT TRAN

        END

    END TRY
    BEGIN CATCH

        IF @@TRANCOUNT > 0 ROLLBACK TRAN
        THROW

    END CATCH
END
";

                    try
                    {
                        _ = new SqlExec(new SqlExecSettings
                        {
                            SqlConnectionString = Settings.SqlConnectionString,
                            CommandString = create
                        });
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlExec() => Failed to create [dbo].[usp_datastore_{tableName}_UpdateDescendantLineages]; {e.Message}");
                    }
                }

                #endregion
            }

            else
            {
                throw new Exception("DataStore() => No SQL connection string provided");
            }
        }
    }

    #endregion

    #region Public Properties

    public DataStoreSettings Settings { get; private set; } = null!;

    public List<DataStoreIndexedColumn> IndexedColumns { get; } = new ();

    public List<DataStoreTableSetting> TableDefinitions { get; } = new ();
    
    public ObjectPool<StringBuilder> StringBuilderPool { get; } = new DefaultObjectPoolProvider().CreateStringBuilderPool();
    
    public double LastTotalReadTimeMs { get; set; }
    public double LastTotalWriteTimeMs { get; set; }
    public double LastTotalTimeMs { get; set; }

    public static JsonSerializerOptions JsonSerializerOptions => new () 
    {
        WriteIndented = false,
        PropertyNameCaseInsensitive = false,
        IncludeFields = true,
        MaxDepth = 100
    };
    
    public string SqlServerVersion { get; private set; } = null!;

    #region Min and Max Values for SQL Types 
    
    public static readonly DateTimeOffset DateTimeOffsetMinValue = DateTimeOffset.MinValue.ToUniversalTime();
    public static readonly DateTimeOffset DateTimeOffsetMaxValue = DateTimeOffset.MaxValue.ToUniversalTime();
    
    public static readonly decimal DecimalMinValue = (decimal)-9999999999999.99999;
    public static readonly decimal DecimalMaxValue = (decimal)9999999999999.99999;        
    
    public static readonly double DoubleMinValue = Convert.ToDouble("-1.79E+308");
    public static readonly double DoubleMaxValue = Convert.ToDouble("1.79E+308");

    public static readonly int IntMinValue = int.MinValue;
    public static readonly int IntMaxValue = int.MaxValue;        
    
    public static readonly Int64 LongMinValue = Int64.MinValue;
    public static readonly Int64 LongMaxValue = Int64.MaxValue;
    
    #endregion

    #endregion

    #region Methods
    
    #region Save
    
    /// <summary>
    /// Save one or more DsObject objects to the database.
    /// The existing objects are updated from the database after save.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// await dataStore.SaveManyAsync(typeof(User), new List<User> { user1, user2, user3 });
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="type">Object type</param>
    /// <param name="dsos">List of DsObject objects</param>
    /// <param name="maxDegreeOfParallelism">Number of concurrent save requests</param>
    public async Task SaveManyAsync(Type type, IEnumerable<DsObject> dsos, int maxDegreeOfParallelism = -1)
    {
        var timer = new Stopwatch();
        var totalTimer = new Stopwatch();
        var existingDsos = new List<DsObject>();
        var tableName = GenerateTableName(type);
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism == -1 ? (Environment.ProcessorCount > 0 ? Environment.ProcessorCount : 4) : maxDegreeOfParallelism
        };

        totalTimer.Start();
        
        LastTotalReadTimeMs = 0;
        LastTotalWriteTimeMs = 0;

        if (TableUsesLineageFeatures(tableName))
        {
            var idList = dsos.Select(s => s.Id).ToList();

            existingDsos = await GetManyAsync(type, 1, int.MaxValue, new DsQuery().IdInList(idList));
        }
        
        var partitioner = new DataStorePartitioner<DsObject>(dsos);

        timer.Start();
        
        Parallel.ForEach(partitioner, parallelOptions, dso =>
        {
            var writeQuery = StringBuilderPool.Get();
            var timer2 = new Stopwatch();

            timer2.Start();
            
            dso.Serialize(this, generateJsonDocument: false);

            LastTotalWriteTimeMs -= timer2.ElapsedMilliseconds;
            
            writeQuery.Clear();
            writeQuery.Append($@"
SET NOCOUNT ON

DECLARE @object_id uniqueidentifier
DECLARE @object_parent_id uniqueidentifier
DECLARE @json nvarchar(max)
DECLARE @existing_object_id uniqueidentifier
DECLARE @current_parent_id uniqueidentifier

SET @object_id = {QuotedGuidOrNull(dso.Id)}
SET @object_parent_id = {QuotedGuidOrNull(dso.ParentId)}
SET @json = {QuotedOrNull(dso.Json.Replace("'", "''"))}
SET @existing_object_id = (SELECT TOP 1 [Id] FROM [dbo].[datastore_{tableName}] WHERE [Id] = @object_id)
SET @current_parent_id = (SELECT TOP 1 [ParentId] FROM [dbo].[datastore_{tableName}] WHERE [Id] = @object_id)

IF @existing_object_id IS NOT NULL BEGIN

    UPDATE [dbo].[datastore_{tableName}]
    SET
        [json_data] = @json
    WHERE [Id] = @object_id

END
ELSE BEGIN

    INSERT INTO [dbo].[datastore_{tableName}]
    (
        [json_data]
    )
    VALUES
    (
        @json
    )

END
");

            try
            {
                _ = new SqlExec(new SqlExecSettings
                {
                    SqlConnectionString = Settings.SqlConnectionString,
                    CommandString = writeQuery.ToString()
                });
            }

            catch (Exception e)
            {
                throw new Exception($"DataStore.SaveAsync(Write Objects) => {e.Message}");
            }

            StringBuilderPool.Return(writeQuery);
        });

        LastTotalWriteTimeMs += timer.ElapsedMilliseconds;

        if (TableUsesLineageFeatures(tableName))
        {
            timer.Reset();
            timer.Start();
            partitioner = new DataStorePartitioner<DsObject>(dsos);
            
            Parallel.ForEach(partitioner, parallelOptions, dso =>
            {
                var lineageOverheadTimer = new Stopwatch();

                lineageOverheadTimer.Start();
                
                var existingDso = existingDsos.FirstOrDefault(d => d.Id == dso.Id);

                LastTotalWriteTimeMs -= lineageOverheadTimer.ElapsedMilliseconds;
                
                if (existingDso == null || existingDso.ParentId != dso.ParentId)
                {
                    lineageOverheadTimer.Reset();
                    lineageOverheadTimer.Start();
                    
                    var lineageQuery = StringBuilderPool.Get();

                    lineageQuery.Append(@"
BEGIN
");
                    if (dso.ParentId != null)
                    {
                        lineageQuery.Append($@"
    EXEC [dbo].[usp_datastore_{tableName}_UpdateDescendantLineages] @parent_object_id = '{dso.ParentId}'
");
                    }

                    else
                    {
                        lineageQuery.Append($@"
    EXEC [dbo].[usp_datastore_{tableName}_UpdateDescendantLineages] @parent_object_id = '{dso.Id}'
");
                    }

                    lineageQuery.Append(@"
END
");

                    LastTotalWriteTimeMs -= lineageOverheadTimer.ElapsedMilliseconds;

                    try
                    {
                        _ = new SqlExec(new SqlExecSettings
                        {
                            SqlConnectionString = Settings.SqlConnectionString,
                            CommandString = lineageQuery.ToString()
                        });
                    }
                
                    catch (Exception e)
                    {
                        throw new Exception($"DataStore.SaveAsync(Update Lineages) => {e.Message}");
                    }
                    
                    StringBuilderPool.Return(lineageQuery);
                }
            });
            
            LastTotalWriteTimeMs += timer.ElapsedMilliseconds;
        }

        var select = $@"
SELECT TOP {dsos.Count()} * FROM [dbo].[datastore_{tableName}] WHERE [Id] IN ({(dsos.Any() ? $"'{string.Join("','", dsos.Select(d => d.Id))}'" : "")})
";

        var readOverheadTimer = new Stopwatch();

        timer.Reset();
        timer.Start();
        
        await using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
        {
            await using (var sqlCmd = new SqlCommand())
            {
                try
                {
                    sqlCmd.CommandText = select;
                    sqlCmd.Connection = sqlConnection;
                    await sqlConnection.OpenAsync();

                    if (sqlConnection.State == ConnectionState.Open)
                    {
                        await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                        {
                            if (sqlDataReader.HasRows)
                            {
                                while (sqlDataReader.Read())
                                {
                                    var newDso = await ReadObjectData(type, sqlDataReader);

                                    readOverheadTimer.Reset();
                                    readOverheadTimer.Start();

                                    var dsoRef = dsos.FirstOrDefault(d => d.Id == newDso?.Id);
        
                                    if (dsoRef != null)
                                    {
                                        await newDso.CloneObjectToAsync(dsoRef);
                                    }

                                    LastTotalReadTimeMs -= readOverheadTimer.ElapsedMilliseconds;
                                }
                            }
                        }
                    }

                    else
                    {
                        throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                    }
                }

                catch (Exception e)
                {
                    throw new Exception($"SqlConnection() => {e.Message}");
                }
            }
        }
        
        LastTotalReadTimeMs += timer.ElapsedMilliseconds;
        LastTotalTimeMs = totalTimer.ElapsedMilliseconds;
    }

    /// <summary>
    /// Save one or more DsObject objects to the database.
    /// The existing objects are updated from the database after save.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// await dataStore.SaveManyAsync<User>(new List<User> { user1, user2, user3 });
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dsos">List of DsObject objects</param>
    /// <param name="maxDegreeOfParallelism">Number of concurrent save requests</param>
    public async Task SaveManyAsync<T>(IEnumerable<T> dsos, int maxDegreeOfParallelism = -1)
    {
        await SaveManyAsync(typeof(T), dsos.Cast<DsObject>().ToList(), maxDegreeOfParallelism);
    }

    /// <summary>
    /// Save one or more DsObject objects to the database.
    /// The existing objects are updated from the database after save.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.SaveMany<User>(new List<User> { user1, user2, user3 });
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dsos">List of DsObject objects</param>
    /// <param name="maxDegreeOfParallelism">Number of concurrent save requests</param>
    public void SaveMany<T>(IEnumerable<T> dsos, int maxDegreeOfParallelism = -1)
    {
        Tasks.WaitForTaskToComplete(SaveManyAsync(dsos), maxDegreeOfParallelism);
    }
    
    /// <summary>
    /// Save an object to the database.
    /// The existing object is updated from the database after save.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// await dataStore.SaveAsync<User>(user);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso">DsObject object</param>
    /// <param name="maxDegreeOfParallelism">Number of concurrent save requests</param>
    public async Task SaveAsync(DsObject dso, int maxDegreeOfParallelism = -1)
    {
        await SaveManyAsync(dso.GetType(), new List<DsObject> { dso }, maxDegreeOfParallelism);
    }
    
    /// <summary>
    /// Save an object to the database.
    /// The existing object is updated from the database after save.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.Save<User>(user);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso">DsObject object</param>
    /// <param name="maxDegreeOfParallelism">Number of concurrent save requests</param>
    public void Save(DsObject dso, int maxDegreeOfParallelism = -1)
    {
        Tasks.WaitForTaskToComplete(SaveManyAsync(dso.GetType(), new List<DsObject> { dso }), maxDegreeOfParallelism);
    }
    
    #endregion

    #region Get Single
    
    /// <summary>
    /// Get a single DsObject from the database by its Id.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var user = dataStore.GetSingleById<User>(id);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="id">DsObject Id value</param>
    /// <returns>DsObject object</returns>
    // ReSharper disable once UnusedMember.Global
    public T? GetSingleById<T>(Guid? id) where T : class
    {
        return Tasks.WaitForTaskToComplete(GetSingleByIdAsync(typeof(T), id)) as T;
    }

    /// <summary>
    /// Get a single DsObject from the database by its Id.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var user = dataStore.GetSingleById(typeof(User), id);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="id">DsObject Id value</param>
    /// <returns>DsObject object</returns>
    // ReSharper disable once UnusedMember.Global
    public DsObject? GetSingleById(Type type, Guid? id)
    {
        return Tasks.WaitForTaskToComplete(GetSingleByIdAsync(type, id));
    }
    
    /// <summary>
    /// Get a single DsObject from the database by its Id.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var user = await dataStore.GetSingleByIdAsync<User>(id);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="id">DsObject Id value</param>
    /// <returns>DsObject object</returns>
    public async Task<T?> GetSingleByIdAsync<T>(Guid? id) where T : class
    {
        return await GetSingleByIdAsync(typeof(T), id) as T;
    }

    /// <summary>
    /// Get a single DsObject from the database by its Id.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var user = await dataStore.GetSingleByIdAsync(typeof(User), id);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="type"></param>
    /// <param name="id">DsObject Id value</param>
    /// <returns>DsObject object</returns>
    public async Task<DsObject?> GetSingleByIdAsync(Type type, Guid? id)
    {
        return await GetSingleAsync(type, new DsQuery().IdEqualTo(id));
    }
    
   /// <summary>
    /// Get a single DsObject from the database default table.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var dso = dataStore.GetSingle<User>(new DsQuery("Manager").StringProp<User>(p => p.Email).EqualTo("michael@example.com"));
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="query"></param>
    /// <returns>DsObject object</returns>
    // ReSharper disable once UnusedMember.Global
    public T? GetSingle<T>(DsQuery? query) where T : class
   {
        return Tasks.WaitForTaskToComplete(GetSingleAsync(typeof(T), query)) as T;
    }
    
    /// <summary>
    /// Get a single DsObject from the database default table.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var dso = await dataStore.GetSingleAsync<User>(new DsQuery("Manager").StringProp<User>(p => p.Email).EqualTo("michael@example.com"));
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="query"></param>
    /// <returns>DsObject object</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<T?> GetSingleAsync<T>(DsQuery? query) where T : class
    {
        var result = await GetManyAsync(typeof(T), 1, 1, query);

        return result.Any() ? result.First() as T : default;
    }

    /// <summary>
    /// Get a single DsObject from the database default table.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var dso = await dataStore.GetSingleAsync<User>(typeof(User), new DsQuery("Manager").StringProp<User>(p => p.Email).EqualTo("michael@example.com"));
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="query"></param>
    /// <returns>DsObject object</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<DsObject?> GetSingleAsync(Type type, DsQuery? query)
    {
        var result = await GetManyAsync(type, 1, 1, query);

        return result.Any() ? result.First() : default;
    }
    
    #endregion
    
    #region Get Many

    /// <summary>
    /// Get one or more paged, queried, and ordered DataObjects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetManyAsync(typeof(User), 1, 50, 
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="page">Results page number to return</param>
    /// <param name="perPage">Number of results per page</param>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <returns>List of DsObject items</returns>
    public async Task<List<DsObject>> GetManyAsync(Type type, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        var dsos = new List<DsObject>();
        var totalTimer = new Stopwatch();

        totalTimer.Start();
        
        try
        {
            query ??= new DsQuery();
            orderBy ??= new DsOrderBy();

            var tableName = GenerateTableName(type);
            var offset = (page * perPage) - perPage + 1;
            var queryCrossApplyClause = GetCrossApplyStatement(type, query.CrossApplyWithFieldNames, query.CrossApplyWithFields);
            var orderByCrossApplyClause = GetCrossApplyStatement(type, orderBy.CrossApplyWithFieldNames, orderBy.CrossApplyWithFields);
            var select = $@"
SET NOCOUNT ON

;WITH pg AS
(
    SELECT [Id], ROW_NUMBER() OVER(ORDER BY {orderBy.OrderByClause}) AS [rn]
    FROM [dbo].[datastore_{tableName}] {orderByCrossApplyClause.StringIndent("    ")}
    WHERE [Id] IN (
        SELECT DISTINCT [Id]
        FROM [dbo].[datastore_{tableName}] {queryCrossApplyClause.StringIndent("        ")}
        WHERE ({query.WhereClause})
    )
), 
pg2 AS
(
    SELECT {(perPage == int.MaxValue ? string.Empty : $"TOP {perPage} ")}[Id], [rn]
    FROM pg {(perPage == int.MaxValue ? string.Empty : $"WHERE [rn] BETWEEN {offset} AND {offset + perPage}")} 
)

SELECT {(perPage == int.MaxValue ? string.Empty : $"TOP {perPage} ")}[json_data], [rn]
FROM [dbo].[datastore_{tableName}] c
INNER JOIN pg2 ON pg2.[Id] = c.[Id]
";

            LastTotalReadTimeMs = 0;
            LastTotalWriteTimeMs = 0;

            var timer = new Stopwatch();
            var overheadTimer = new Stopwatch();

            timer.Start();
            
            await using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
            {
                await using (var sqlCmd = new SqlCommand())
                {
                    try
                    {
                        sqlCmd.CommandText = select;
                        sqlCmd.Connection = sqlConnection;
                        await sqlConnection.OpenAsync();

                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                            {
                                if (sqlDataReader.HasRows)
                                {
                                    while (sqlDataReader.Read())
                                    {
                                        var dso = await ReadObjectData(type, sqlDataReader);

                                        overheadTimer.Reset();
                                        overheadTimer.Start();

                                        if (dso != null) dsos.Add(dso);

                                        LastTotalReadTimeMs -= overheadTimer.ElapsedMilliseconds;
                                    }
                                }
                            }
                        }

                        else
                        {
                            throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                        }
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlConnection() => {e.Message}");
                    }
                }
            }

            LastTotalReadTimeMs += timer.ElapsedMilliseconds;
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.GetManyAsync() => {e.Message}");
        }

        LastTotalTimeMs = totalTimer.ElapsedMilliseconds;
        
        return dsos;
    }
    
    /// <summary>
    /// Get one or more paged, queried, and ordered DsObject objects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = dataStore.GetMany<User>(1, 50, 
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="page">Results page number to return</param>
    /// <param name="perPage">Number of results per page</param>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <returns>List of DsObject items</returns>
    // ReSharper disable once UnusedMember.Global
    public List<T> GetMany<T>(int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null) where T : class
    {
        return Tasks.WaitForTaskToComplete(GetManyAsync<T>(page, perPage, query, orderBy));
    }

    /// <summary>
    /// Get one or more paged, queried, and ordered DataObjects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetManyAsync<User>(1, 50, 
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="page">Results page number to return</param>
    /// <param name="perPage">Number of results per page</param>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <returns>List of DsObject items</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<T>> GetManyAsync<T>(int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        var results = await GetManyAsync(typeof(T), page, perPage, query, orderBy);

        return results.Cast<T>().ToList();
    }    

    /// <summary>
    /// Get one or more paged, queried, and ordered DataObjects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetManyAsync<User>( 
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <returns>List of DsObject items</returns>
    public async Task<List<T>> GetManyAsync<T>(DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        return await GetManyAsync<T>(1, int.MaxValue, query, orderBy);
    }    

    /// <summary>
    /// Get one or more paged, queried, and ordered DataObjects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetManyAsync(typeof(User), 1, 50, 
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <returns>List of DsObject items</returns>
    public async Task<List<DsObject>> GetManyAsync(Type type, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        return await GetManyAsync(type, 1, int.MaxValue, query, orderBy);
    }
    
    #endregion
    
    #region Get Count

    /// <summary>
    /// Return the number of found objects based on a query
    /// </summary>
    /// <param name="type"></param>
    /// <param name="query"></param>
    /// <returns></returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<long> GetCountAsync(Type type, DsQuery? query = null)
    {
        var count = 0;
        var totalTimer = new Stopwatch();

        totalTimer.Start();
        
        try
        {
            query ??= new DsQuery();

            var tableName = GenerateTableName(type);
            var queryCrossApplyClause = GetCrossApplyStatement(type, query.CrossApplyWithFieldNames, query.CrossApplyWithFields);
            var select = $@"
SELECT COUNT(*) AS [count]
FROM 
(
    SELECT DISTINCT [Id]
    FROM [dbo].[datastore_{tableName}] {queryCrossApplyClause}
    WHERE ({query.WhereClause})
) o
";
            LastTotalReadTimeMs = 0;
            LastTotalWriteTimeMs = 0;

            var timer = new Stopwatch();

            timer.Start();

            await using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
            {
                await using (var sqlCmd = new SqlCommand())
                {
                    try
                    {
                        sqlCmd.CommandText = select;
                        sqlCmd.Connection = sqlConnection;
                        await sqlConnection.OpenAsync();

                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                            {
                                if (sqlDataReader.HasRows)
                                {
                                    await sqlDataReader.ReadAsync();
                                    count = await sqlDataReader.SqlSafeGetIntAsync("count");
                                }
                            }
                        }

                        else
                        {
                            throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                        }
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlConnection() => {e.Message}");
                    }
                }
            }

            LastTotalReadTimeMs += timer.ElapsedMilliseconds;
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.GetCountAsync() => {e.Message}");
        }

        LastTotalTimeMs = totalTimer.ElapsedMilliseconds;
        
        return count;
    }
    
    /// <summary>
    /// Return the objects based on a query
    /// </summary>
    /// <returns></returns>
    // ReSharper disable once UnusedMember.Global
    public long GetCount<T>()
    {
        return Tasks.WaitForTaskToComplete(GetCountAsync<T>());
    }
    
    /// <summary>
    /// Return the number of found objects based on a query
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    // ReSharper disable once UnusedMember.Global
    public long GetCount<T>(DsQuery? query)
    {
        return Tasks.WaitForTaskToComplete(GetCountAsync<T>(query));
    }

    /// <summary>
    /// Return the number of found objects based on a query
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<long> GetCountAsync<T>(DsQuery? query = null)
    {
        return await GetCountAsync(typeof(T), query);
    }

    #endregion
    
    #region Get Descendants

    /// <summary>
    /// Get one or more paged, queried, and ordered descendant DsObjects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetDescendantsAsync(
    ///     typeof(User),
    ///     ancestorId,
    ///     1, 50,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="type"></param>
    /// <param name="ancestorId"></param>
    /// <param name="page">Results page number to return</param>
    /// <param name="perPage">Number of results per page</param>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <param name="depth">Number of levels of descendants to retrieve; -1 retrieves all descendants</param>
    /// <returns>List of DsObject items</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<DsObject>> GetDescendantsAsync(Type type, Guid? ancestorId, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null, int depth = -1)
    {
        var dsos = new List<DsObject>();
        var totalTimer = new Stopwatch();

        totalTimer.Start();

        if (ancestorId != null)
        {
            try
            {
                query ??= new DsQuery();
                orderBy ??= new DsOrderBy().DepthAscending();

                var tableName = GenerateTableName(type);
                var queryCrossApplyClause = GetCrossApplyStatement(type, query.CrossApplyWithFieldNames, query.CrossApplyWithFields);
                var orderByCrossApplyClause = GetCrossApplyStatement(type, orderBy.CrossApplyWithFieldNames, orderBy.CrossApplyWithFields);
                var offset = (page * perPage) - perPage;
                var select = $@"
DECLARE @depth bigint = (SELECT TOP 1 [Depth] FROM [dbo].[datastore_{tableName}] WHERE [Id] = '" + ancestorId.Value + $@"')
DECLARE @maxdepth bigint

IF @depth IS NOT NULL BEGIN

SET @maxdepth = @depth + {(depth == -1 ? int.MaxValue : depth)}

IF @maxdepth > {int.MaxValue} BEGIN
    SET @maxdepth = {int.MaxValue}
END

;WITH descendant AS (
	SELECT TOP 1
            [Id],
			[ParentId],
			[Depth]
	FROM [dbo].[datastore_{tableName}]
	WHERE [Id] = '{ancestorId.Value}'

	UNION ALL

	SELECT  ft.[Id],
			ft.[ParentId],
			ft.[Depth]
	FROM [dbo].[datastore_{tableName}] ft {queryCrossApplyClause}
	JOIN descendant d
	ON ft.[ParentId] = d.[Id]
    WHERE ft.[Depth] >= @depth AND ft.[Depth] <= @maxdepth AND ({query.WhereClause}) 
)

SELECT {(page == 1 && perPage == 1 ? "TOP 1 *" : "*")}
FROM [dbo].[datastore_{tableName}] {orderByCrossApplyClause}
WHERE [Id] IN (
    SELECT DISTINCT [Id]
    FROM descendant
) AND [Id] <> '{ancestorId.Value}'
ORDER BY {orderBy.OrderByClause}
OFFSET {offset} ROWS FETCH NEXT {perPage} ROWS ONLY
END
";

                LastTotalReadTimeMs = 0;
                LastTotalWriteTimeMs = 0;

                var timer = new Stopwatch();
                var overheadTimer = new Stopwatch();

                timer.Start();

                await using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
                {
                    await using (var sqlCmd = new SqlCommand())
                    {
                        try
                        {
                            sqlCmd.CommandText = select;
                            sqlCmd.Connection = sqlConnection;
                            await sqlConnection.OpenAsync();

                            if (sqlConnection.State == ConnectionState.Open)
                            {
                                await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                                {
                                    if (sqlDataReader.HasRows)
                                    {
                                        while (sqlDataReader.Read())
                                        {
                                            var dso = await ReadObjectData(type, sqlDataReader);

                                            overheadTimer.Reset();
                                            overheadTimer.Start();
                                            
                                            if (dso != null) dsos.Add(dso);

                                            LastTotalReadTimeMs -= overheadTimer.ElapsedMilliseconds;
                                        }
                                    }
                                }
                            }

                            else
                            {
                                throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                            }
                        }

                        catch (Exception e)
                        {
                            throw new Exception($"SqlConnection() => {e.Message}");
                        }
                    }
                }

                LastTotalReadTimeMs = timer.ElapsedMilliseconds;
            }

            catch (Exception e)
            {
                throw new Exception($"DataStore.GetDescendantsAsync() => {e.Message}");
            }
        }

        else
        {
            throw new Exception("DataStore.GetDescendantsAsync() => Ancestor Id is null");
        }

        LastTotalTimeMs = totalTimer.ElapsedMilliseconds;
        
        return dsos;
    }
    
    /// <summary>
    /// Get one or more paged, queried, and ordered descendant DsObjects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = dataStore.GetDescendants<User>(
    ///     ancestorId,
    ///     1, 50,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="ancestorId"></param>
    /// <param name="page">Results page number to return</param>
    /// <param name="perPage">Number of results per page</param>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <param name="depth">Number of levels of descendants to retrieve; -1 retrieves all descendants</param>
    /// <returns>List of DsObject items</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    // ReSharper disable once UnusedMember.Global
    public List<T> GetDescendants<T>(Guid? ancestorId, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null, int depth = -1)
    {
        return Tasks.WaitForTaskToComplete(GetDescendantsAsync<T>(ancestorId, page, perPage, query, orderBy, depth));
    }        

    /// <summary>
    /// Get one or more paged, queried, and ordered descendant DsObjects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetDescendantsAsync<User>(
    ///     ancestorId,
    ///     1, 50,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="ancestorId"></param>
    /// <param name="page">Results page number to return</param>
    /// <param name="perPage">Number of results per page</param>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <param name="depth">Number of levels of descendants to retrieve; -1 retrieves all descendants</param>
    /// <returns>List of DsObject items</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<T>> GetDescendantsAsync<T>(Guid? ancestorId, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null, int depth = -1)
    {
        var results = await GetDescendantsAsync(typeof(T), ancestorId, page, perPage, query, orderBy, depth);

        return results.Cast<T>().ToList();
    }    
    
    /// <summary>
    /// Get one or more paged, queried, and ordered descendant DsObjects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = dataStore.GetDescendants<User>(
    ///     dso,
    ///     1, 50,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso"></param>
    /// <param name="page">Results page number to return</param>
    /// <param name="perPage">Number of results per page</param>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <param name="depth">Number of levels of descendants to retrieve; -1 retrieves all descendants</param>
    /// <returns>List of DsObject items</returns>
    // ReSharper disable once UnusedMember.Global
    public List<T> GetDescendants<T>(DsObject? dso, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null, int depth = -1)
    {
        return Tasks.WaitForTaskToComplete(GetDescendantsAsync<T>(dso, page, perPage, query, orderBy, depth));
    }
    
    /// <summary>
    /// Get one or more paged, queried, and ordered descendant DsObjects from the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetDescendantsAsync<User>(
    ///     dso,
    ///     1, 50,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso"></param>
    /// <param name="page">Results page number to return</param>
    /// <param name="perPage">Number of results per page</param>
    /// <param name="query">DsQuery() object for filtering results</param>
    /// <param name="orderBy">DsOrderBy() object for ordering results</param>
    /// <param name="depth">Number of levels of descendants to retrieve; -1 retrieves all descendants</param>
    /// <returns>List of DsObject items</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<T>> GetDescendantsAsync<T>(DsObject? dso, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null, int depth = -1)
    {
        return await GetDescendantsAsync<T>(dso?.Id, page, perPage, query, orderBy, depth);
    }

    #endregion
    
    #region Get Children

    /// <summary>
    /// Get a List of the passed object's children DsObjects.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetChildrenAsync(
    ///     typeof(User),
    ///     parentId,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="type"></param>
    /// <param name="parentId"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of child objects or null on error (see LastException for details)</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<DsObject>> GetChildrenAsync(Type type, Guid? parentId, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        var results = new List<DsObject>();

        if (parentId == null) return results;

        query ??= new DsQuery();
        orderBy ??= new DsOrderBy();

        var newQuery = new DsQuery(query);
            
        newQuery = (newQuery.HasQuery ? newQuery.AND().ParentIdEqualTo(parentId) : newQuery.ParentIdEqualTo(parentId));
                
        results = await GetManyAsync(type, page, perPage, newQuery, orderBy);

        return results;
    }
    
    /// <summary>
    /// Get a List of the passed object's children DsObjects.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = dataStore.GetChildren<User>(
    ///     parentId,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="parentId"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of child objects or null on error (see LastException for details)</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    // ReSharper disable once UnusedMember.Global
    public List<T> GetChildren<T>(Guid? parentId, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        return Tasks.WaitForTaskToComplete(GetChildrenAsync<T>(parentId, page, perPage, query, orderBy));
    }

    /// <summary>
    /// Get a List of the passed object's children DsObjects.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetChildrenAsync<User>(
    ///     parentId,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="parentId"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of child objects or null on error (see LastException for details)</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<T>> GetChildrenAsync<T>(Guid? parentId, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        var results = await GetChildrenAsync(typeof(T), parentId, page, perPage, query, orderBy);

        return results.Cast<T>().ToList();
    }    
    
    /// <summary>
    /// Get a List of the passed object's children DsObjects.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = dataStore.GetChildren<User>(
    ///     dso,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of child objects or null on error (see LastException for details)</returns>
    // ReSharper disable once UnusedMember.Global
    public List<T> GetChildren<T>(DsObject? dso, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        return Tasks.WaitForTaskToComplete(GetChildrenAsync<T>(dso, page, perPage, query, orderBy));
    }

    /// <summary>
    /// Get a List of the passed object's children DsObjects.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetChildrenAsync<User>(
    ///     dso,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of child objects or null on error (see LastException for details)</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<T>> GetChildrenAsync<T>(DsObject? dso, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        return await GetChildrenAsync<T>(dso?.Id, page, perPage, query, orderBy);
    }
    
    #endregion
    
    #region Get Ancestors

    /// <summary>
    /// Get a List of the passed object's ancestors from parent back to the eldest progenitor.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetAncestorsAsync(
    ///     typeof(User),
    ///     dso,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of ancestor objects or null on error (see LastException for details)</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<DsObject>> GetAncestorsAsync(Type type, DsObject? dso, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        var results = new List<DsObject>();

        if (dso == null) return results;

        query ??= new DsQuery();
        orderBy ??= new DsOrderBy().DepthDescending();

        var newQuery = new DsQuery(query);

        var newDso = await GetSingleByIdAsync(type, dso.Id);

        if (newDso != null)
        {
            dso.ParentId = newDso.ParentId;
            dso.Depth = newDso.Depth;
            dso.Lineage = newDso.Lineage;
        }

        if (dso.Lineage == null || dso.Lineage.Count < 1) return results;

        newQuery = newQuery.HasQuery ? newQuery.AND().IdInList(dso.Lineage).AND().NOT().IdEqualTo(dso.Id) : newQuery.IdInList(dso.Lineage).AND().NOT().IdEqualTo(dso.Id);
        results = await GetManyAsync(type, page, perPage, newQuery, orderBy);

        return results;
    }
    
    /// <summary>
    /// Get a List of the passed object Id's ancestors from parent back to the eldest progenitor.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = dataStore.GetAncestors<User>(
    ///     dsoId,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dsoId"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of ancestor objects or null on error (see LastException for details)</returns>
    // ReSharper disable once UnusedMember.Global
    public List<T> GetAncestors<T>(Guid? dsoId, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        return Tasks.WaitForTaskToComplete(GetAncestorsAsync<T>(dsoId, page, perPage, query, orderBy));
    }

    /// <summary>
    /// Get a List of the passed object Id's ancestors from parent back to the eldest progenitor.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetAncestorsAsync<User>(
    ///     dsoId,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dsoId"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of ancestor objects or null on error (see LastException for details)</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<T>> GetAncestorsAsync<T>(Guid? dsoId, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        var dso = await GetSingleByIdAsync(typeof(T), dsoId);

        return await GetAncestorsAsync<T>(dso, page, perPage, query, orderBy);
    }

    /// <summary>
    /// Get a List of the passed object's ancestors from parent back to the eldest progenitor.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = dataStore.GetAncestors<User>(
    ///     dso,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of ancestor objects or null on error (see LastException for details)</returns>
    // ReSharper disable once UnusedMember.Global
    public List<T> GetAncestors<T>(DsObject? dso, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        return Tasks.WaitForTaskToComplete(GetAncestorsAsync<T>(dso, page, perPage, query, orderBy));
    }

    /// <summary>
    /// Get a List of the passed object's ancestors from parent back to the eldest progenitor.
    /// Does not include the passed object.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.GetAncestorsAsync<User>(
    ///     dso,
    ///     new DsQuery("Manager")
    ///         .NumberProp<User,int>(p => p.Age).GreaterThan(49)
    ///         .AND()
    ///         .GroupBegin()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(51)
    ///             .OR()
    ///             .NumberProp<User,int>(p => p.Age).EqualTo(52)
    ///         .GroupEnd(),
    ///     new DsOrderBy()
    ///         .DepthAscending()
    ///         .Prop<User,int>(p => p.Age).Ascending()
    ///     );
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso"></param>
    /// <param name="page">Results page to return</param>
    /// <param name="perPage">Number of objects per page to return</param>
    /// <param name="query"></param>
    /// <param name="orderBy"></param>
    /// <returns>A list of ancestor objects or null on error (see LastException for details)</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<T>> GetAncestorsAsync<T>(DsObject? dso, int page = 1, int perPage = int.MaxValue, DsQuery? query = null, DsOrderBy? orderBy = null)
    {
        var results = await GetAncestorsAsync(typeof(T), dso, page, perPage, query, orderBy);

        return results.Cast<T>().ToList();
    }    

    #endregion

    #region Lineages

    /// <summary>
    /// Update the lineages and depths for the parent and all descendants.
    /// If parentId is null, update lineages for all objects.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.UpdateLineages(typeof(User), user.ParentId);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="type"></param>
    /// <param name="parentId">Parent Id for all its descendants, or null for all objects</param>
    public void UpdateLineages(Type type, Guid? parentId = null)
    {
        var tableName = GenerateTableName(type);
        var totalTimer = new Stopwatch();

        totalTimer.Start();

        LastTotalReadTimeMs = 0;
        LastTotalWriteTimeMs = 0;

        var timer = new Stopwatch();

        timer.Start();

        if (parentId != null)
        {
            try
            {
                _ = new SqlExec(new SqlExecSettings
                {
                    SqlConnectionString = Settings.SqlConnectionString,
                    CommandString = $"EXEC [dbo].[usp_datastore_{tableName}_UpdateDescendantLineages] @parent_object_id = {QuotedGuidOrNull(parentId)}"
                });
            }

            catch (Exception e)
            {
                throw new Exception($"DataStore.UpdateLineagesAsync(parentId) => {e.Message}");
            }
        }

        else
        {
            try
            {
                _ = new SqlExec(new SqlExecSettings
                {
                    SqlConnectionString = Settings.SqlConnectionString,
                    CommandString = $"EXEC [dbo].[usp_datastore_{tableName}_UpdateDescendantLineages] @parent_object_id = NULL"
                });
            }

            catch (Exception e)
            {
                throw new Exception($"DataStore.UpdateLineagesAsync() => {e.Message}");
            }
        }

        LastTotalWriteTimeMs += timer.ElapsedMilliseconds;
        LastTotalTimeMs = totalTimer.ElapsedMilliseconds;
    }
    
    /// <summary>
    /// Update the lineages and depths for the parent and all descendants.
    /// If parentId is null, update lineages for all objects.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.UpdateLineages<User>(user.ParentId);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="parentId">Parent Id for all its descendants, or null for all objects</param>
    // ReSharper disable once MemberCanBePrivate.Global
    public void UpdateLineages<T>(Guid? parentId = null)
    {
        UpdateLineages(typeof(T), parentId);
    }

    /// <summary>
    /// Update the lineages and depths for all objects.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.UpdateAllLineages<User>();
    /// ]]>
    /// </code>
    /// </example>
    public void UpdateAllLineages<T>()
    {
        UpdateAllLineages(typeof(T));
    }
    
    /// <summary>
    /// Update the lineages and depths for all objects in a table.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.UpdateAllLineages(typeof(User));
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="type"></param>
    /// <param name="parentId">Parent Id for all its descendants, or null for all objects</param>
    public void UpdateAllLineages(Type type)
    {
        UpdateLineages(type);
    }

    #endregion
    
    #region Delete

    /// <summary>
    /// Mark one or more objects deleted in the database
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.DeleteAsync(
    ///     typeof(User),
    ///     new DsQuery("Manager")
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
    /// <param name="type"></param>
    /// <param name="query"></param>
    /// <returns>List of deleted items</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<DsObject>> DeleteAsync(Type type, DsQuery? query)
    {
        var dsos = new List<DsObject>();
        var totalTimer = new Stopwatch();

        totalTimer.Start();

        try
        {
            query ??= new DsQuery();

            var tableName = GenerateTableName(type); 
            var queryCrossApplyClause = GetCrossApplyStatement(type, query.CrossApplyWithFieldNames, query.CrossApplyWithFields);
            var select = $@"
SET NOCOUNT ON

DECLARE @matches TABLE ([Id] uniqueidentifier) 

INSERT INTO @matches
SELECT o.[Id]
FROM [dbo].[datastore_{tableName}] o {queryCrossApplyClause}";
            if (query.WhereClause.StringHasValue())
            {
                select += $@"                    
WHERE ({query.WhereClause})";
            }
            
            select += $@"    

DECLARE @hasData int = (select 1 where exists (select 1 from @matches))

IF @hasData = 1 BEGIN                

    UPDATE [dbo].[datastore_{tableName}]
    SET [json_data] = JSON_MODIFY([json_data], '$.IsDeleted', true)
    FROM [dbo].[datastore_{tableName}] o {queryCrossApplyClause}
    WHERE o.[Id] IN (SELECT [Id] FROM @matches)

    SELECT *
    FROM [dbo].[datastore_{tableName}] o {queryCrossApplyClause}
    WHERE o.[Id] IN (SELECT [Id] FROM @matches)

END
";
            
            LastTotalReadTimeMs = 0;
            LastTotalWriteTimeMs = 0;

            var timer = new Stopwatch();
            var overheadTimer = new Stopwatch();

            timer.Start();

            await using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
            {
                await using (var sqlCmd = new SqlCommand())
                {
                    try
                    {
                        sqlCmd.CommandText = select;
                        sqlCmd.Connection = sqlConnection;
                        await sqlConnection.OpenAsync();

                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                            {
                                LastTotalWriteTimeMs += timer.ElapsedMilliseconds;

                                if (sqlDataReader.HasRows)
                                {
                                    timer.Reset();
                                    timer.Start();
                                    
                                    while (sqlDataReader.Read())
                                    {
                                        var dso = await ReadObjectData(type, sqlDataReader);

                                        overheadTimer.Reset();
                                        overheadTimer.Start();
                                        
                                        if (dso != null) dsos.Add(dso);

                                        LastTotalReadTimeMs -= overheadTimer.ElapsedMilliseconds;
                                    }
                                    
                                    LastTotalReadTimeMs += timer.ElapsedMilliseconds;
                                }
                            }
                        }

                        else
                        {
                            throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                        }
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlConnection() => {e.Message}");
                    }
                }
            }
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.DeleteAsync() => {e.Message}");
        }

        LastTotalTimeMs = totalTimer.ElapsedMilliseconds;
        
        return dsos;
    }
    
    /// <summary>
    /// Mark an object as deleted in the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.Delete<User>(user);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso">DsObject object</param>
    public void Delete(DsObject? dso)
    {
        Tasks.WaitForTaskToComplete(DeleteAsync(dso));
    }

    /// <summary>
    /// Mark an object as deleted in the database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// await dataStore.DeleteAsync<User>(user);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso">DsObject object</param>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task DeleteAsync(DsObject? dso)
    {
        if (dso != null)
        {
            dso.IsDeleted = true;

            await SaveAsync(dso);
        }

        else
        {
            throw new Exception("DataStore.DeleteAsync() => DsObject is null");
        }
    }

    /// <summary>
    /// Mark an object as deleted in the database based on its Id
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.Delete<User>(user.Id);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="objectId">DataStore Object Id</param>
    // ReSharper disable once MemberCanBePrivate.Global
    public void Delete<T>(Guid? objectId) where T : class
    {
        Tasks.WaitForTaskToComplete(DeleteAsync<T>(objectId));
    }

    /// <summary>
    /// Mark an object as deleted in the database based on its Id
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// await dataStore.DeleteAsync<User>(user.Id);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="objectId">DataStore Object Id</param>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task DeleteAsync<T>(Guid? objectId) where T : class
    {
        try
        {
            if (objectId != null)
            {
                var dso = await GetSingleByIdAsync<T>(objectId);

                await DeleteAsync(dso as DsObject);
            }

            else
            {
                throw new Exception("DataStore.DeleteAsync() => DsObject Id is null");
            }
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.DeleteAsync() => {e.Message}");
        }
    }

    /// <summary>
    /// Mark one or more objects deleted in the database
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = dataStore.Delete<User>(
    ///     new DsQuery("Manager")
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
    /// <param name="query"></param>
    /// <returns>List of deleted items</returns>
    // ReSharper disable once UnusedMember.Global
    public List<T> Delete<T>(DsQuery? query)
    {
        return Tasks.WaitForTaskToComplete(DeleteAsync<T>(query));
    }

    /// <summary>
    /// Mark one or more objects deleted in the database
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.DeleteAsync<User>(
    ///     new DsQuery("Manager")
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
    /// <param name="query"></param>
    /// <returns>List of deleted items</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<T>> DeleteAsync<T>(DsQuery? query)
    {
        var results = await DeleteAsync(typeof(T), query);

        return results.Cast<T>().ToList();
    }

    #endregion
    
    #region Undelete

    /// <summary>
    /// Mark one or more objects not deleted in the database
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.UndeleteAsync(
    ///     typeof(User),
    ///     new DsQuery("Manager")
    ///         .IsDeleted()
    ///         .AND()
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
    /// <param name="query"></param>
    /// <returns>List of undeleted objects</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<DsObject>> UndeleteAsync(Type type, DsQuery? query)
    {
        var dsos = new List<DsObject>();
        var totalTimer = new Stopwatch();

        totalTimer.Start();

        try
        {
            query ??= new DsQuery();

            var tableName = GenerateTableName(type);
            var queryCrossApplyClause = GetCrossApplyStatement(type, query.CrossApplyWithFieldNames, query.CrossApplyWithFields);
            
            var select = $@"
SET NOCOUNT ON

DECLARE @matches TABLE ([Id] uniqueidentifier) 

INSERT INTO @matches
SELECT o.[Id]
FROM [dbo].[datastore_{tableName}] o {queryCrossApplyClause}";

            if (query.WhereClause.StringHasValue())
            {
                select += $@"                    
WHERE ({query.WhereClause})";
            }
            
            select += $@"    

DECLARE @hasData int = (select 1 where exists (select 1 from @matches))

IF @hasData = 1 BEGIN                

    UPDATE [dbo].[datastore_{tableName}]
    SET [json_data] = JSON_MODIFY([json_data], '$.IsDeleted', false)
    FROM [dbo].[datastore_{tableName}] o {queryCrossApplyClause}
    WHERE o.[Id] IN (SELECT [Id] FROM @matches)

    SELECT *
    FROM [dbo].[datastore_{tableName}] o {queryCrossApplyClause}
    WHERE o.[Id] IN (SELECT [Id] FROM @matches)

END
";

            LastTotalReadTimeMs = 0;
            LastTotalWriteTimeMs = 0;

            var timer = new Stopwatch();
            var overheadTimer = new Stopwatch();

            timer.Start();

            await using (var sqlConnection = new SqlConnection(Settings.SqlConnectionString))
            {
                await using (var sqlCmd = new SqlCommand())
                {
                    try
                    {
                        sqlCmd.CommandText = select;
                        sqlCmd.Connection = sqlConnection;
                        await sqlConnection.OpenAsync();

                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                            {
                                LastTotalWriteTimeMs += timer.ElapsedMilliseconds;
                                
                                if (sqlDataReader.HasRows)
                                {
                                    timer.Reset();
                                    timer.Start();

                                    while (sqlDataReader.Read())
                                    {
                                        var dso = await ReadObjectData(type, sqlDataReader);

                                        overheadTimer.Reset();
                                        overheadTimer.Start();
                                        
                                        if (dso != null) dsos.Add(dso);

                                        LastTotalReadTimeMs -= overheadTimer.ElapsedMilliseconds;
                                    }

                                    LastTotalReadTimeMs += timer.ElapsedMilliseconds;
                                }
                            }
                        }

                        else
                        {
                            throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                        }
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlConnection() => {e.Message}");
                    }
                }
            }

            LastTotalWriteTimeMs = timer.ElapsedMilliseconds;
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.UndeleteAsync() => {e.Message}");
        }

        LastTotalTimeMs = totalTimer.ElapsedMilliseconds;
        
        return dsos;
    }
    
    /// <summary>
    /// Mark an object as active (undelete) in the database and reload object properties into the passed object
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.Undelete<User>(user);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso">DsObject object</param>
    public void Undelete(DsObject? dso)
    {
        Tasks.WaitForTaskToComplete(UndeleteAsync(dso));
    }

    /// <summary>
    /// Mark an object as active (undelete) in the database and reload object properties into the passed object
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// await dataStore.UndeleteAsync<User>(user);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso">DsObject object</param>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task UndeleteAsync(DsObject? dso)
    {
        if (dso != null)
        {
            dso.IsDeleted = false;
            
            await SaveAsync(dso);
        }

        else
        {
            throw new Exception("DataStore.UndeleteAsync() => DsObject is null");
        }
    }

    /// <summary>
    /// Mark an object as active (undelete) in the database based on its Id
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.Undelete<User>(user.Id);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="objectId">DataStore Object Id</param>
    // ReSharper disable once MemberCanBePrivate.Global
    public void Undelete<T>(Guid? objectId) where T : class
    {
        Tasks.WaitForTaskToComplete(UndeleteAsync<T>(objectId));
    }

    /// <summary>
    /// Mark an object as active (undelete) in the database based on its Id
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// await dataStore.UndeleteAsync<User>(user.Id);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="objectId">DataStore Object Id</param>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task UndeleteAsync<T>(Guid? objectId) where T : class
    {
        try
        {
            if (objectId != null)
            {
                var dso = await GetSingleByIdAsync<T>(objectId);

                await UndeleteAsync(dso as DsObject);
            }

            else
            {
                throw new Exception("DataStore.UndeleteAsync() => objectId is null");
            }
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.UndeleteAsync() => {e.Message}");
        }
    }

    /// <summary>
    /// Mark one or more objects not deleted in the database
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = dataStore.Undelete<User>(
    ///     new DsQuery("Manager")
    ///         .IsDeleted()
    ///         .AND()
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
    /// <param name="query"></param>
    /// <returns>List of undeleted objects</returns>
    // ReSharper disable once UnusedMember.Global
    public List<T> Undelete<T>(DsQuery? query)
    {
        return Tasks.WaitForTaskToComplete(UndeleteAsync<T>(query));
    }

    /// <summary>
    /// Mark one or more objects not deleted in the database
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var users = await dataStore.UndeleteAsync<User>(
    ///     new DsQuery("Manager")
    ///         .IsDeleted()
    ///         .AND()
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
    /// <param name="query"></param>
    /// <returns>List of undeleted objects</returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public async Task<List<T>> UndeleteAsync<T>(DsQuery? query)
    {
        var results = await UndeleteAsync(typeof(T), query);

        return results.Cast<T>().ToList();
    }

    #endregion
    
    #region Populate DsObject from SqlDataReader
    
    /// <summary>
    /// Load object data from an open SqlDataReader (e.g. LastDate, etc.).
    /// </summary>
    /// <param name="type">Model type to return</param>
    /// <param name="sqlDataReader">Open and ready SqlDataReader after a Read() operation</param>
    public async Task<DsObject?> ReadObjectData(Type type, SqlDataReader? sqlDataReader)
    {
        var timer = new Stopwatch();

        if (sqlDataReader == null) throw new Exception("DataStore.ReadObjectData() => SqlDataReader is null");

        try
        {
            timer.Start();

            var newDso = Activator.CreateInstance(type);

            LastTotalReadTimeMs -= timer.ElapsedMilliseconds;
            
            if (newDso is not DsObject tmpDso) throw new Exception("DataStore.ReadObjectData() => Cannot create DsObject");

            var json = await sqlDataReader.SqlSafeGetStringAsync("json_data");

            timer.Reset();
            timer.Start();

            await tmpDso.Deserialize(json, this);

            LastTotalReadTimeMs -= timer.ElapsedMilliseconds;
            
            tmpDso.IsNew = false;
            tmpDso.RowNum = await sqlDataReader.SqlSafeGetLongAsync("rn");

            return tmpDso;
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.ReadObjectData() => {e.Message}");
        }
    }

    /// <summary>
    /// Load object data from an open SqlDataReader (e.g. LastDate, etc.).
    /// </summary>
    /// <param name="sqlDataReader">Open and ready SqlDataReader after a Read() operation</param>
    public async Task<T?> ReadObjectData<T>(SqlDataReader? sqlDataReader) where T : class
    {
        return await ReadObjectData(typeof(T), sqlDataReader) as T;
    }
    
    #endregion
    
    #region Table Helpers
    
    /// <summary>
    /// Returns true if the specified table uses lineage features 
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public bool TableUsesLineageFeatures(string tableName)
    {
        var result = false;

        if (tableName.StringIsEmpty()) throw new Exception("DataStore.TableUsesLineageFeatures() => Table name cannot be empty");
        
        if (TableDefinitions.Any() == false) return false;

        var tableDef = TableDefinitions.FirstOrDefault(c => c.TableName.Equals(tableName, StringComparison.InvariantCultureIgnoreCase));

        if (tableDef != null)
        {
            result = tableDef.UseLineageFeatures;
        }

        return result;
    }    
    
    /// <summary>
    /// Load all DataStore table names into a list.
    /// </summary>
    /// <returns></returns>
    public static List<string?> GetTableNames(string? sqlConnectionString)
    {
        var result = new List<string?>();

        if (sqlConnectionString == null) return result;

        using (var sqlConnection = new SqlConnection(sqlConnectionString))
        {
            using (var sqlCmd = new SqlCommand())
            {
                try
                {
                    sqlCmd.CommandText = @"SELECT [name] FROM dbo.sysobjects WHERE OBJECTPROPERTY(id, N'IsTable') = 1 AND [name] LIKE 'datastore_%'";
                    sqlCmd.Connection = sqlConnection;
                    sqlConnection.Open();

                    if (sqlConnection.State == ConnectionState.Open)
                    {
                        using (var sqlDataReader = sqlCmd.ExecuteReader())
                        {
                            if (sqlDataReader.HasRows)
                            {
                                while (sqlDataReader.Read())
                                {
                                    result.Add(sqlDataReader.SqlSafeGetString("name"));
                                }
                            }
                        }
                    }

                    else
                    {
                        throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                    }
                }

                catch (Exception e)
                {
                    throw new Exception($"SqlConnection() => {e.Message}");
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Generate the SQL table name for a given model Type.
    /// Inherited classes are used as prefixes with underscores (e.g. BaseClass_MyClass).
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public static string GenerateTableName(Type type)
    {
        var tableName = type.Name;
        var parentType = type.BaseType;

        while (parentType != null && parentType != typeof(DsObject))
        {
            tableName = $"{parentType.Name}_{tableName}";
            parentType = parentType.BaseType;
        }

        return tableName;
    }
    
    #endregion

    #region Get Sproc Names

    /// <summary>
    /// Load all DataStore SPROC names into a list.
    /// </summary>
    /// <returns></returns>
    public static List<string?> GetSprocNames(string? sqlConnectionString)
    {
        var result = new List<string?>();

        if (sqlConnectionString == null) return result;

        using (var sqlConnection = new SqlConnection(sqlConnectionString))
        {
            using (var sqlCmd = new SqlCommand())
            {
                try
                {
                    sqlCmd.CommandText = @"SELECT [name] FROM dbo.sysobjects WHERE OBJECTPROPERTY(id, N'IsProcedure') = 1 AND [name] LIKE 'usp_datastore_%'";
                    sqlCmd.Connection = sqlConnection;
                    sqlConnection.Open();

                    if (sqlConnection.State == ConnectionState.Open)
                    {
                        using (var sqlDataReader = sqlCmd.ExecuteReader())
                        {
                            if (sqlDataReader.HasRows)
                            {
                                while (sqlDataReader.Read())
                                {
                                    result.Add(sqlDataReader.SqlSafeGetString("name"));
                                }
                            }
                        }
                    }

                    else
                    {
                        throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                    }
                }

                catch (Exception e)
                {
                    throw new Exception($"SqlConnection() => {e.Message}");
                }
            }
        }

        return result;
    }

    #endregion

    #region Get Func Names

    /// <summary>
    /// Load all DataStore Func names into a list.
    /// </summary>
    /// <returns></returns>
    public static List<string?> GetFuncNames(string? sqlConnectionString)
    {
        var result = new List<string?>();

        if (sqlConnectionString == null) return result;

        using (var sqlConnection = new SqlConnection(sqlConnectionString))
        {
            using (var sqlCmd = new SqlCommand())
            {
                try
                {
                    sqlCmd.CommandText = @"SELECT [name] FROM dbo.sysobjects WHERE OBJECTPROPERTY(id, N'IsScalarFunction') = 1 AND [name] LIKE 'udf_datastore_%'";
                    sqlCmd.Connection = sqlConnection;
                    sqlConnection.Open();

                    if (sqlConnection.State == ConnectionState.Open)
                    {
                        using (var sqlDataReader = sqlCmd.ExecuteReader())
                        {
                            if (sqlDataReader.HasRows)
                            {
                                while (sqlDataReader.Read())
                                {
                                    result.Add(sqlDataReader.SqlSafeGetString("name"));
                                }
                            }
                        }
                    }

                    else
                    {
                        throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                    }
                }

                catch (Exception e)
                {
                    throw new Exception($"SqlConnection() => {e.Message}");
                }
            }
        }

        return result;
    }

    #endregion
    
    #region Schema Purges

    /// <summary>
    /// Permanently drops all unused tables and their associated procedures, functions, etc.
    /// </summary>
    /// <param name="settings"></param>
    public void DeleteUnusedTables(DataStoreSettings settings)
    {
        Tasks.WaitForTaskToComplete(DeleteUnusedTablesAsync(settings));
    }
    
    /// <summary>
    /// Permanently drops all unused tables and their associated procedures, functions, etc.
    /// </summary>
    /// <param name="settings"></param>
    public async Task DeleteUnusedTablesAsync(DataStoreSettings settings)
    {
        var tables = GetTableNames(settings.SqlConnectionString);

        foreach (var tableName in tables)
        {
            if (tableName != null && TableDefinitions.FirstOrDefault(t => t.TableName.Equals(tableName.TrimStringStart("datastore_") ?? string.Empty, StringComparison.InvariantCultureIgnoreCase)) == null)
            {
                await DeleteDatabaseObjectsAsync(settings.SqlConnectionString, tableName.TrimStringStart("datastore_") ?? string.Empty);
            }
        }
    }

    /// <summary>
    /// Drop all database schema objects used by DataStore.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// DataStore.DeleteDatabaseObjects(sqlConnectionString);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="sqlConnectionString"></param>
    /// <param name="onlyTableName"></param>
    public static void DeleteDatabaseObjects(string sqlConnectionString, string onlyTableName = "")
    {
        Tasks.WaitForTaskToComplete(DeleteDatabaseObjectsAsync(sqlConnectionString, onlyTableName));
    }

    /// <summary>
    /// Drop all database schema objects used by DataStore.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// await DataStore.DeleteDatabaseObjectsAsync(sqlConnectionString);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="sqlConnectionString"></param>
    /// <param name="onlyTableName"></param>
    public static async Task DeleteDatabaseObjectsAsync(string sqlConnectionString, string onlyTableName = "")
    {
        var tableNames = GetTableNames(sqlConnectionString);
        var sprocNames = GetSprocNames(sqlConnectionString);
        var funcNames = GetFuncNames(sqlConnectionString);
        var drops = string.Empty;

        if (onlyTableName.StringHasValue())
        {
            tableNames.Clear();
            tableNames.Add("datastore_" + onlyTableName);

            if (sprocNames.Any())
            {
                sprocNames.RemoveAll(s => s != null && s.StartsWith("usp_datastore_" + onlyTableName + "_") == false);
            }

            if (funcNames.Any())
            {
                funcNames.RemoveAll(s => s != null && s.StartsWith("udf_datastore_" + onlyTableName + "_") == false);
            }
        }

        else
        {
            tableNames.Add("datastore");
        }

        foreach (var tableName in tableNames)
        {
            if (sqlConnectionString == null) continue;

            await using (var sqlConnection = new SqlConnection(sqlConnectionString))
            {
                await using (var sqlCmd = new SqlCommand())
                {
                    try
                    {
                        sqlCmd.CommandText = $"SELECT TOP 1 * FROM dbo.sysobjects WHERE id = object_id(N'{tableName}') AND OBJECTPROPERTY(id, N'IsTable') = 1";
                        sqlCmd.Connection = sqlConnection;
                        await sqlConnection.OpenAsync();

                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                            {
                                if (sqlDataReader.HasRows)
                                {
                                    drops += @$"DROP TABLE [dbo].[{tableName}]
";
                                }
                            }
                        }

                        else
                        {
                            throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                        }
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlConnection() => {e.Message}");
                    }
                }
            }
        }
        
        foreach (var sprocName in sprocNames)
        {
            if (sqlConnectionString == null) continue;

            await using (var sqlConnection = new SqlConnection(sqlConnectionString))
            {
                await using (var sqlCmd = new SqlCommand())
                {
                    try
                    {
                        sqlCmd.CommandText = $"SELECT TOP 1 * FROM dbo.sysobjects WHERE id = object_id(N'{sprocName}') AND OBJECTPROPERTY(id, N'IsProcedure') = 1";
                        sqlCmd.Connection = sqlConnection;
                        await sqlConnection.OpenAsync();

                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                            {
                                if (sqlDataReader.HasRows)
                                {
                                    drops += $@"DROP PROCEDURE [dbo].[{sprocName}]
";
                                }
                            }
                        }

                        else
                        {
                            throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                        }
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlConnection() => {e.Message}");
                    }
                }
            }
        }

        foreach (var funcName in funcNames)
        {
            if (sqlConnectionString == null) continue;

            await using (var sqlConnection = new SqlConnection(sqlConnectionString))
            {
                await using (var sqlCmd = new SqlCommand())
                {
                    try
                    {
                        sqlCmd.CommandText = $"SELECT TOP 1 * FROM dbo.sysobjects WHERE id = object_id(N'{funcName}') AND OBJECTPROPERTY(id, N'IsScalarFunction') = 1";
                        sqlCmd.Connection = sqlConnection;
                        await sqlConnection.OpenAsync();

                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                            {
                                if (sqlDataReader.HasRows)
                                {
                                    drops += $@"DROP FUNCTION [dbo].[{funcName}]
";
                                }
                            }
                        }

                        else
                        {
                            throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                        }
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlConnection() => {e.Message}");
                    }
                }
            }
        }

        if (drops.StringHasValue())
        {
            if (sqlConnectionString != null)
            {
                try
                {
                    _ = new SqlExec(new SqlExecSettings
                    {
                        SqlConnectionString = sqlConnectionString,
                        CommandString = drops
                    });
                }

                catch (Exception e)
                {
                    throw new Exception($"DataStore.DeleteDatabaseObjectsAsync() => {e.Message}");
                }
            }
        }
    }

    /// <summary>
    /// Drop all non-table database schema objects used by DataStore.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// await DataStore.DeleteDatabaseHelperObjectsAsync(sqlConnectionString);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="sqlConnectionString"></param>
    public static async Task DeleteDatabaseHelperObjectsAsync(string sqlConnectionString)
    {
        var sprocNames = GetSprocNames(sqlConnectionString);
        var funcNames = GetFuncNames(sqlConnectionString);
        var drops = string.Empty;

        foreach (var sprocName in sprocNames)
        {
            if (sqlConnectionString == null) continue;

            await using (var sqlConnection = new SqlConnection(sqlConnectionString))
            {
                await using (var sqlCmd = new SqlCommand())
                {
                    try
                    {
                        sqlCmd.CommandText = $"SELECT TOP 1 * FROM dbo.sysobjects WHERE id = object_id(N'{sprocName}') AND OBJECTPROPERTY(id, N'IsProcedure') = 1";
                        sqlCmd.Connection = sqlConnection;
                        await sqlConnection.OpenAsync();

                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                            {
                                if (sqlDataReader.HasRows)
                                {
                                    drops += $@"DROP PROCEDURE [dbo].[{sprocName}]
";
                                }
                            }
                        }

                        else
                        {
                            throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                        }
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlConnection() => {e.Message}");
                    }
                }
            }
        }

        foreach (var funcName in funcNames)
        {
            if (sqlConnectionString == null) continue;

            await using (var sqlConnection = new SqlConnection(sqlConnectionString))
            {
                await using (var sqlCmd = new SqlCommand())
                {
                    try
                    {
                        sqlCmd.CommandText = $"SELECT TOP 1 * FROM dbo.sysobjects WHERE id = object_id(N'{funcName}') AND OBJECTPROPERTY(id, N'IsScalarFunction') = 1";
                        sqlCmd.Connection = sqlConnection;
                        await sqlConnection.OpenAsync();

                        if (sqlConnection.State == ConnectionState.Open)
                        {
                            await using (var sqlDataReader = await sqlCmd.ExecuteReaderAsync())
                            {
                                if (sqlDataReader.HasRows)
                                {
                                    drops += $@"DROP FUNCTION [dbo].[{funcName}]
";
                                }
                            }
                        }

                        else
                        {
                            throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                        }
                    }

                    catch (Exception e)
                    {
                        throw new Exception($"SqlConnection() => {e.Message}");
                    }
                }
            }
        }

        if (drops.StringHasValue())
        {
            if (sqlConnectionString != null)
            {
                try
                {
                    _ = new SqlExec(new SqlExecSettings
                    {
                        SqlConnectionString = sqlConnectionString,
                        CommandString = drops
                    });
                }

                catch (Exception e)
                {
                    throw new Exception($"DataStore.DeleteDatabaseHelperObjectsAsync() => {e.Message}");
                }
            }
        }
    }

    /// <summary>
    /// Truncate (permanently delete) all data in the DataStore tables
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// DataStore.PurgeAllData(sqlConnectionString);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="sqlConnectionString"></param>
    public static void PurgeAllData(string? sqlConnectionString)
    {
        try
        {
            if (sqlConnectionString != null)
            {
                var tableNames = GetTableNames(sqlConnectionString);
                var query = tableNames.Aggregate("",
                    (current, tableName) => current + $"TRUNCATE TABLE [dbo].[{tableName}]\r\n");

                try
                {
                    _ = new SqlExec(new SqlExecSettings
                    {
                        SqlConnectionString = sqlConnectionString,
                        CommandString = query
                    });
                }

                catch (Exception e)
                {
                    throw new Exception($"DataStore.PurgeAllDataAsync() => {e.Message}");
                }
            }
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.PurgeAllDataAsync() => {e.Message}");
        }
    }

    #endregion

    #region Database Maintenance
    
    /// <summary>
    /// Moves data to the beginning of the database file so it can be compacted later.
    /// Also rebuilds all indexes on existing tables.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// var dataStore = new DataStore(...);
    /// dataStore.ShrinkDatabase();
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="sqlConnectionString"></param>
    public void ShrinkDatabase()
    {
        if (Settings.SqlConnectionString.StringHasValue())
        {
            if (Settings.DatabaseName.StringHasValue())
            {
                var commands = @$"
DBCC SHRINKDATABASE ({Settings.DatabaseName}, 20)
";

                try
                {
                    _ = new SqlExec(new SqlExecSettings
                    {
                        SqlConnectionString = Settings.SqlConnectionString,
                        CommandString = commands
                    });
                }

                catch (Exception e)
                {
                    throw new Exception($"DataStore.ShrinkDatabase() => {e.Message}");
                }

                commands = string.Empty;

                if (TableDefinitions.Any())
                {
                    foreach (var tableName in GetTableNames(Settings.SqlConnectionString))
                    {
                        commands += $@"
ALTER INDEX ALL ON [dbo].[{tableName}]
REBUILD WITH (FILLFACTOR = 80, SORT_IN_TEMPDB = ON, STATISTICS_NORECOMPUTE = ON)
";
                    }
                }

                try
                {
                    _ = new SqlExec(new SqlExecSettings
                    {
                        SqlConnectionString = Settings.SqlConnectionString,
                        CommandString = commands
                    });
                }

                catch (Exception e)
                {
                    throw new Exception($"DataStore.ShrinkDatabase() => {e.Message}");
                }

                commands = @"
DECLARE @file_id nvarchar(2000)
DECLARE @logfile_id nvarchar(2000)

SET @file_id = (SELECT FILE_IDEX((SELECT TOP (1) [name] FROM sys.database_files WHERE [type] = 0)) AS 'FileID')
SET @logfile_id = (SELECT FILE_IDEX((SELECT TOP (1) [name] FROM sys.database_files WHERE [type] = 1)) AS 'LogFileID')

DBCC SHRINKFILE(@file_id, 0)
DBCC SHRINKFILE(@logfile_id, 0)
";

                try
                {
                    _ = new SqlExec(new SqlExecSettings
                    {
                        SqlConnectionString = Settings.SqlConnectionString,
                        CommandString = commands
                    });
                }

                catch
                {
                    // ignored
                }
            }
        }
    }
    
    #endregion
    
    #region DsObject Purges
    
    /// <summary>
    /// Purge an object from the database.
    /// Does not clear out the passed DsObject.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.PurgeObject<User>(user);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="dso">DsObject object</param>
    // ReSharper disable once MemberCanBePrivate.Global
    public void PurgeObject<T>(DsObject? dso)
    {
        PurgeObject(typeof(T), dso);
    }

    /// <summary>
    /// Purge an object from the database.
    /// Does not clear out the passed DsObject.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// dataStore.PurgeObject(typeof(User), user);
    /// ]]>
    /// </code>
    /// </example>
    /// <param name="type"></param>
    /// <param name="dso">DsObject object</param>
    // ReSharper disable once MemberCanBePrivate.Global
    public void PurgeObject(Type type, DsObject? dso)
    {
        try
        {
            if (dso != null)
            {
                var tableName = GenerateTableName(type);
                
                try
                {
                    _ = new SqlExec(new SqlExecSettings
                    {
                        SqlConnectionString = Settings.SqlConnectionString,
                        CommandString = $"DELETE FROM [dbo].[datastore_{tableName}] WHERE [Id] = '{dso.Id}'"
                    });
                }

                catch (Exception e)
                {
                    throw new Exception($"DataStore.PurgeObjectAsync() => {e.Message}");
                }
            }

            else
            {
                throw new Exception("DataStore.PurgeObjectAsync() => DsObject is null");
            }
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.PurgeObjectAsync() => {e.Message}");
        }
    }

    /// <summary>
    /// Purge one or more objects from the database.
    /// </summary>
    /// <param name="query"></param>
    /// <returns></returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public void Purge<T>(DsQuery? query)
    {
        Purge(typeof(T), query);
    }

    /// <summary>
    /// Purge one or more objects from the database.
    /// </summary>
    /// <param name="type"></param>
    /// <param name="query"></param>
    /// <returns></returns>
    // ReSharper disable once MemberCanBePrivate.Global
    public void Purge(Type type, DsQuery? query)
    {
        query ??= new DsQuery();

        var tableName = GenerateTableName(type);
        var queryCrossApplyClause = GetCrossApplyStatement(type, query.CrossApplyWithFieldNames, query.CrossApplyWithFields);
        var select = $@"
SET NOCOUNT ON

DECLARE @matches TABLE ([Id] uniqueidentifier) 

INSERT INTO @matches
SELECT o.[Id]
FROM [dbo].[datastore_{tableName}] o {queryCrossApplyClause}";

        if (query.WhereClause.StringHasValue())
        {
            select += $@"                    
WHERE ({query.WhereClause})";
        }
        
        select += $@"                    

DECLARE @hasData int = (select 1 where exists (select 1 from @matches))

IF @hasData = 1 BEGIN
DELETE FROM [dbo].[datastore_{tableName}]
WHERE [Id] IN (SELECT [Id] FROM @matches)
END
";
            
        try
        {
            _ = new SqlExec(new SqlExecSettings
            {
                SqlConnectionString = Settings.SqlConnectionString,
                CommandString = select
            });
        }

        catch (Exception e)
        {
            throw new Exception($"DataStore.Purge() => {e.Message}");
        }
    }
    
    #endregion
    
    #region Serialization Helpers

    public JsonSerializerContext? GetSerializerContext(string tableName)
    {
        if (tableName.StringIsEmpty()) throw new Exception("DataStore.GetSerializerContext() => Table name cannot be empty");

        var context = TableDefinitions.FirstOrDefault(t => t.TableName.Equals(tableName, StringComparison.InvariantCultureIgnoreCase))?.ModelSerializerContext;
        
        return context;
    }

    /// <summary>
    /// Return a Guid? value as JSON-quoted or null.
    /// </summary>
    /// <param name="value">GUID? value to format</param>
    /// <returns>Value suitable for JSON</returns>
    public static string JsonQuotedGuidOrNull(Guid? value)
    {
        return value != null && value != new Guid() ? "\"" + value + "\"" : "null";
    }

    /// <summary>
    /// Return a string value as JSON-quoted or null.
    /// </summary>
    /// <param name="value">String value to format</param>
    /// <returns>Value suitable for a JSON</returns>
    public static string JsonQuotedOrNull(string? value)
    {
        return value != null ? "\"" + value + "\"" : "null";
    }
    
    #endregion
    
    #region SQL Helpers

    /// <summary>
    /// Return a Guid? value as SQL-quoted or NULL, for use in SQL statements.
    /// </summary>
    /// <param name="value">GUID? value to format</param>
    /// <returns>Value suitable for a SQL statement</returns>
    public static string QuotedGuidOrNull(Guid? value)
    {
        return value != null ? "'" + value + "'" : "NULL";
    }

    /// <summary>
    /// Return a string value as SQL-quoted or NULL, for use in SQL statements.
    /// </summary>
    /// <param name="value">String value to format</param>
    /// <returns>Value suitable for a SQL statement</returns>
    public static string QuotedOrNull(string? value)
    {
        return value != null ? "'" + value + "'" : "NULL";
    }

    private string GetCrossApplyStatement(Type type, Dictionary<string,string> crossApplyWithFieldNames, Dictionary<string,string> crossApplyWithFields)
    {
        var crossApplyIncludes = new Dictionary<string,string>();

        foreach (var (propertyPath, _) in crossApplyWithFieldNames)
        {
            if (IndexedColumns.FirstOrDefault(p => p.ColumnName == JsonPathToColumnName(propertyPath) && p.ModelType == type) == null)
            {
                var pathPrefix = (propertyPath.Split('.').Length > 1 ? propertyPath[..propertyPath.LastIndexOf('.')] : string.Empty).Trim('$');

                if (crossApplyIncludes.ContainsKey(propertyPath)) continue;
                
                crossApplyIncludes.Add(propertyPath, $@"
CROSS APPLY OPENJSON(json_data{(pathPrefix == string.Empty ? "" : $", '{CleanSqlJsonPath(pathPrefix)}'")})
WITH ({crossApplyWithFields[propertyPath]})");
            }
        }

        return crossApplyIncludes.Aggregate(string.Empty, (current, cai) => current + cai.Value);
    }

    #endregion
    
    #region Index Helpers

    /// <summary>
    /// Convert a JSON Path to a property path:
    /// e.g. $."Emails"[*]."Email" => Emails.Email
    /// </summary>
    /// <param name="jsonPath"></param>
    /// <returns></returns>
    public static string JsonPathToPropertyPath(string jsonPath)
    {
        return (jsonPath.Replace("\"", string.Empty).TrimStringStart("$") ?? string.Empty).Trim('.').Replace("[*]", ".").Replace("..", ".");
    }
    
    /// <summary>
    /// Convert a JSON Path to a SQL column name:
    /// e.g. $."Emails"[*]."Email" => Emails_Email
    /// </summary>
    /// <param name="jsonPath"></param>
    /// <returns></returns>
    public static string JsonPathToColumnName(string jsonPath)
    {
        return JsonPathToPropertyPath(jsonPath).Replace('.', '_');
    }

    /// <summary>
    /// Properly format JSON path for SQL use
    /// </summary>
    /// <param name="jsonPath"></param>
    /// <returns></returns>
    public static string CleanSqlJsonPath(string jsonPath)
    {
        var result = jsonPath
            .Replace("\"", string.Empty)
            .TrimStringStart("$")?
            .Replace("[*]", ".")
            .Replace("..", ".")
            .Trim('.');
        
        return $"$.\"{result?.Replace(".", "\".\"")}\"";
    }
    
    public void ExplorePropertiesAndFields(Type type)
    {
        ExplorePropertyOrField(type, type, string.Empty);
    }
    
    public void ExplorePropertyOrField(Type sourceType, Type? propertyOrField, string pathPrefix)
    {
        if (propertyOrField != null)
        {
            foreach (var prop in propertyOrField.GetFields().OrderBy(f => f.DeclaringType != typeof(DsObject)))
            {
                if (prop.IsPublic)
                {
                    if (prop.FieldType.IsSimpleDataType())
                    {
                        var attrs = prop.GetCustomAttributes(true);
                        var skip = false;

                        foreach (var attr in attrs)
                        {
                            var skipAttr = attr is JsonIgnoreAttribute sattr1 ? sattr1 : default;

                            if (skipAttr != null) skip = true;
                        }
                        
                        if (skip == false)
                        {
                            var isIndexed = false;
                            var isUnique = false;
                            var isClusteredSortColumn = false;
                            
                            foreach (var attr in attrs)
                            {
                                var indexedAttr = attr is DsIndexedColumn attr1 ? attr1 : default;
                                var uniqueAttr = attr is DsUniqueIndex attr2 ? attr2 : default;
                                var clusteredAttr = attr is DsClusteredIndex attr3 ? attr3 : default;

                                if (indexedAttr != null) isIndexed = true;
                                if (uniqueAttr != null) isUnique = true;
                                if (clusteredAttr != null) isClusteredSortColumn = true;
                            }

                            if (isIndexed)
                            {
                                IndexedColumns.Add(new DataStoreIndexedColumn
                                {
                                    ModelType = sourceType,
                                    IsClustered = isClusteredSortColumn,
                                    IsUnique = isUnique,
                                    JsonPath =
                                        $"$.{(pathPrefix.StringHasValue() ? pathPrefix + '.' : string.Empty)}\"{prop.Name}\"",
                                    DataType = prop.FieldType
                                });
                            }
                        }
                    }

                    else
                    {
                        if (prop.FieldType.GetConstructor(Type.EmptyTypes) != null)
                        {
                            if (Activator.CreateInstance(prop.FieldType) is IDictionary newDict)
                            {
                                var kvpValueType = newDict.GetType().GenericTypeArguments[1];
                                var attrs = prop.GetCustomAttributes(true);
                                var skip = false;

                                foreach (var attr in attrs)
                                {
                                    var skipAttr = attr is JsonIgnoreAttribute sattr1 ? sattr1 : default;

                                    if (skipAttr != null) skip = true;
                                }
                        
                                if (skip == false)
                                {
                                    foreach (var attr in attrs)
                                    {
                                        var custAttr = attr is DsIndexedColumn attr1 ? attr1 : default;

                                        if (custAttr != null)
                                        {
                                            foreach (var val in custAttr.Value)
                                            {
                                                IndexedColumns.Add(new DataStoreIndexedColumn
                                                {
                                                    ModelType = sourceType,
                                                    JsonPath =
                                                        $"$.{(pathPrefix.StringHasValue() ? pathPrefix + '.' : string.Empty)}\"{prop.Name}\".\"{val}\"",
                                                    DataType = kvpValueType
                                                });
                                            }
                                        }
                                    }
                                }
                            }

                            else if (Activator.CreateInstance(prop.FieldType) is not IList)
                            {
                                ExplorePropertyOrField(sourceType, prop.FieldType,
                                    (pathPrefix.StringHasValue() ? pathPrefix + '.' : string.Empty) + "\"" + prop.Name +
                                    "\"");
                            }
                        }
                    }
                }
            }

            foreach (var prop in propertyOrField.GetProperties().OrderBy(f => f.DeclaringType != typeof(DsObject)))
            {
                if ((prop.GetMethod?.IsPublic ?? false) && prop.CanRead)
                {
                    if (prop.PropertyType.IsSimpleDataType())
                    {
                        var attrs = prop.GetCustomAttributes(true);
                        var skip = false;

                        foreach (var attr in attrs)
                        {
                            var skipAttr = attr is JsonIgnoreAttribute sattr1 ? sattr1 : default;

                            if (skipAttr != null) skip = true;
                        }
                        
                        if (skip == false)
                        {
                            var isIndexed = false;
                            var isUnique = false;
                            var isClusteredSortColumn = false;
                        
                            foreach (var attr in attrs)
                            {
                                var indexedAttr = attr is DsIndexedColumn attr1 ? attr1 : default;
                                var uniqueAttr = attr is DsUniqueIndex attr2 ? attr2 : default;
                                var clusteredAttr = attr is DsClusteredIndex attr3 ? attr3 : default;

                                if (indexedAttr != null) isIndexed = true;
                                if (uniqueAttr != null) isUnique = true;
                                if (clusteredAttr != null) isClusteredSortColumn = true;
                            }

                            if (isIndexed)
                            {
                                IndexedColumns.Add(new DataStoreIndexedColumn
                                {
                                    ModelType = sourceType,
                                    IsClustered = isClusteredSortColumn,
                                    IsUnique = isUnique,
                                    JsonPath =
                                        $"$.{(pathPrefix.StringHasValue() ? pathPrefix + '.' : string.Empty)}\"{prop.Name}\"",
                                    DataType = prop.PropertyType
                                });
                            }
                        }
                    }

                    else
                    {
                        if (prop.PropertyType.GetConstructor(Type.EmptyTypes) != null)
                        {
                            if (Activator.CreateInstance(prop.PropertyType) is IDictionary newDict)
                            {
                                var kvpValueType = newDict.GetType().GenericTypeArguments[1];
                                var attrs = prop.GetCustomAttributes(true);
                                var skip = false;

                                foreach (var attr in attrs)
                                {
                                    var skipAttr = attr is JsonIgnoreAttribute sattr1 ? sattr1 : default;

                                    if (skipAttr != null) skip = true;
                                }
                        
                                if (skip == false)
                                {
                                    foreach (var attr in attrs)
                                    {
                                        var custAttr = attr is DsIndexedColumn attr1 ? attr1 : default;

                                        if (custAttr != null)
                                        {
                                            foreach (var val in custAttr.Value)
                                            {
                                                IndexedColumns.Add(new DataStoreIndexedColumn
                                                {
                                                    ModelType = sourceType,
                                                    JsonPath =
                                                        $"$.{(pathPrefix.StringHasValue() ? pathPrefix + '.' : string.Empty)}\"{prop.Name}\".\"{val}\"",
                                                    DataType = kvpValueType
                                                });
                                            }
                                        }
                                    }
                                }
                            }

                            else if (Activator.CreateInstance(prop.PropertyType) is not IList)
                            {
                                ExplorePropertyOrField(sourceType, prop.PropertyType,
                                    (pathPrefix.StringHasValue() ? pathPrefix + '.' : string.Empty) + "\"" + prop.Name +
                                    "\"");
                            }
                        }
                    }
                }
            }
        }
    }
    
    #endregion
    
    #endregion
}

/// <summary>
/// Custom table settings; used during DataStore instantiation.
/// </summary>
public class DataStoreTableSetting
{
    /// <summary>
    /// Name to use for the custom table.
    /// Allows isolating types of data within separate tables, like "users", "addresses", etc.
    /// Should be alphanumeric, with hyphens or underscores.
    /// Cannot use a system reserved table name.
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Objects will provide tree depth and allow fast queries for ancestors and descendants.
    /// Enabling lineage features will negatively affect write performance in some cases.
    /// Lineage features are not required to assign and query parent objects.
    /// </summary>
    public bool UseLineageFeatures { get; set; }

    /// <summary>
    /// JsonSerializerContext (source generation) for serializing models for this table.
    /// </summary>
    public JsonSerializerContext ModelSerializerContext { get; set; } = null!;
}

/// <summary>
/// Settings for initializing DataStore
/// </summary>
// ReSharper disable once ClassNeverInstantiated.Global
public class DataStoreSettings
{
    /// <summary>
    /// Standard SQL connection string
    /// </summary>
    public string SqlConnectionString { get; init; } = string.Empty;

    public string DatabaseName
    {
        get
        {
            var builder = new System.Data.Common.DbConnectionStringBuilder
            {
                ConnectionString = SqlConnectionString
            };

            if (builder != null)
            {
                if (builder.ContainsKey("database"))
                {
                    return builder["database"] as string ?? string.Empty;
                }
                
                if (builder.ContainsKey("Initial Catalog"))
                {
                    return builder["Initial Catalog"] as string ?? string.Empty;
                }

                return string.Empty;
            }

            return string.Empty;
        }
    }
}

public class DataStoreIndexedColumn
{
    public Type ModelType { get; set; } = typeof(object);
    public string JsonPath { get; set; } = string.Empty;
    public string ColumnName => DataStore.JsonPathToColumnName(JsonPath);
    public Type DataType { get; set; } = typeof(object);
    public bool IsClustered { get; set; }
    public bool IsUnique { get; set; }
    public string ComputedValue => ComputedColumnOutput();
    public string Crc => ((int)(GetComputedValue().CalculateCrc32() + 0)).ToString(); // add integer to CRC to force a change
    
    private string ComputedColumnOutput()
    {
        var result = $"IIF({Crc}={Crc}, {GetComputedValue().TrimStringEnd(" NOT NULL").TrimStringEnd(" PERSISTED")?.Trim() ?? string.Empty}, NULL) PERSISTED{(Nullable.GetUnderlyingType(DataType) != null ? "" : " NOT NULL")}";

        return result;
    }
    
    private string GetComputedValue()
    {
        var computedColumnValue = $"CONVERT(nvarchar(2000),JSON_VALUE([json_data],'{JsonPath}'))";
        
        if (DataType == typeof(Guid) || DataType == typeof(Guid?))
        {
            computedColumnValue = $"CONVERT(uniqueidentifier,JSON_VALUE([json_data],'{JsonPath}'))";
        }

        if (DataType == typeof(bool) || DataType == typeof(bool?))
        {
            computedColumnValue = $"CONVERT(bit,JSON_VALUE([json_data],'{JsonPath}'))";
        }

        if (DataType == typeof(DateTime) || DataType == typeof(DateTimeOffset) || DataType == typeof(DateTime?) || DataType == typeof(DateTimeOffset?))
        {
            computedColumnValue = $"CONVERT(datetimeoffset(7),JSON_VALUE([json_data],'{JsonPath}'),(127))";
        }

        if (DataType == typeof(byte) || DataType == typeof(byte?))
        {
            computedColumnValue = $"CONVERT(tinyint,JSON_VALUE([json_data],'{JsonPath}'))";
        }

        if (DataType == typeof(short) || DataType == typeof(short?))
        {
            computedColumnValue = $"CONVERT(smallint,JSON_VALUE([json_data],'{JsonPath}'))";
        }

        if (DataType == typeof(int) || DataType == typeof(int?))
        {
            computedColumnValue = $"CONVERT(int,JSON_VALUE([json_data],'{JsonPath}'))";
        }

        if (DataType == typeof(long) || DataType == typeof(long?))
        {
            computedColumnValue = $"CONVERT(bigint,JSON_VALUE([json_data],'{JsonPath}'))";
        }
        
        if (DataType == typeof(double) || DataType == typeof(double?))
        {
            computedColumnValue = $"CONVERT(float,JSON_VALUE([json_data],'{JsonPath}'))";
        }
        
        if (DataType == typeof(decimal) || DataType == typeof(decimal?))
        {
            computedColumnValue = $"CONVERT(decimal(19,5),JSON_VALUE([json_data],'{JsonPath}'))";
        }

        return $"{computedColumnValue} PERSISTED{(Nullable.GetUnderlyingType(DataType) != null ? "" : " NOT NULL")}";
    }
}

public class DataStorePartitioner<T> : OrderablePartitioner<T>
{
    private readonly IEnumerator<T> m_input;

    public DataStorePartitioner(IEnumerable<T> input) : base(true, false, true)
    {
         m_input = input.GetEnumerator();
    }
    
    // Must override to return true.
    public override bool SupportsDynamicPartitions => true;

    public override IList<IEnumerator<KeyValuePair<long, T>>> GetOrderablePartitions(int partitionCount)
    {
        var dynamicPartitions = GetOrderableDynamicPartitions();
        var partitions = new IEnumerator<KeyValuePair<long, T>>[partitionCount];

        for (var i = 0; i < partitionCount; i++)
        {
            partitions[i] = dynamicPartitions.GetEnumerator();
        }

        return partitions;
    }

    public override IEnumerable<KeyValuePair<long, T>> GetOrderableDynamicPartitions()
    {
        return new ReaderDynamicPartitions(m_input);
    }

    private class ReaderDynamicPartitions : IEnumerable<KeyValuePair<long, T>>
    {
        // ReSharper disable once FieldCanBeMadeReadOnly.Local
        private object syncObject = new ();
        private bool finished;
        private readonly IEnumerator<T> m_input;
        // ReSharper disable once FieldCanBeMadeReadOnly.Local
        // ReSharper disable once ConvertToConstant.Local
        private int m_pos = 0;

        internal ReaderDynamicPartitions(IEnumerator<T> input)
        {
            m_input = input;
        }

        public IEnumerator<KeyValuePair<long, T>> GetEnumerator()
        {
            while (true)
            {
                var toReturn = new KeyValuePair<long,T>();

                lock (syncObject)
                {
                    if (finished == false && m_input.MoveNext() == false)
                    {
                        finished = true;
                    }
                    
                    if (finished == false)
                    {
                        toReturn = new KeyValuePair<long,T>(m_pos, m_input.Current);
                    }
                }

                if (finished)
                {
                    yield break;
                }

                yield return toReturn;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
