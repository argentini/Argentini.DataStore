using System.Data;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace DataStore;

public class DsMigration
{
    #region Constructors

    public DsMigration(DataStore dataStore)
    {
        DataStore = dataStore;
        CurrentVersion = DataStore.Version;

        using (var sqlConnection = new SqlConnection(DataStore.Settings.SqlConnectionString))
        {
            using (var sqlCmd = new SqlCommand())
            {
                try
                {
                    sqlCmd.CommandText = "SELECT TOP 1 * FROM [dbo].[datastore]";
                    sqlCmd.Connection = sqlConnection;
                    sqlConnection.Open();

                    if (sqlConnection.State == ConnectionState.Open)
                    {
                        using (var sqlDataReader = sqlCmd.ExecuteReader())
                        {
                            if (sqlDataReader.HasRows)
                            {
                                sqlDataReader.Read();
                                CurrentVersion = sqlDataReader.SqlSafeGetDouble("version");
                            }
                        }
                    }

                    else
                    {
                        throw new Exception($"SqlConnection() => Could not open a SQL connection ({sqlConnection.State})");
                    }
                }

                catch
                {
                    // ignored
                }
            }
        }
    }

    #endregion
    
    #region Properties

    private DataStore DataStore { get; }
    private double CurrentVersion { get; }

    #endregion

    #region Methods

    public void RunMigrations()
    {
        if (Math.Abs(DataStore.Version - CurrentVersion) < 0.00001) return;

        // Migrate from 3.0 to 3.1
        if (Math.Abs(CurrentVersion - 3.0) < 0.00001)
        {
            MigrateFrom3_0();
        }
        
        // Migrate from 3.1 to 3.2
        if (Math.Abs(CurrentVersion - 3.1) < 0.00001)
        {
            MigrateFrom3_1();
        }
        
        // Migrate from 3.2 to 3.3
        if (Math.Abs(CurrentVersion - 3.2) < 0.00001)
        {
            UpdateVersion(3.3);
        }
    }

    public void RunPreMigrations()
    {
        if (Math.Abs(DataStore.Version - CurrentVersion) < 0.00001) return;

        // Migrate from 3.1 to 3.2
        if (Math.Abs(CurrentVersion - 3.1) < 0.00001)
        {
            PreMigrateFrom3_1();
        }
        
        // Migrate from 3.2 to 3.3
        if (Math.Abs(CurrentVersion - 3.2) < 0.00001)
        {
            PreMigrateFrom3_2();
        }
    }
    
    private void UpdateVersion(double version)
    {
        try
        {
            _ = new SqlServerExecute(new SqlServerExecuteSettings
            {
                SqlConnectionString = DataStore.Settings.SqlConnectionString,
                CommandString = $@"
truncate table [dbo].[datastore]

insert into [dbo].[datastore] ([version]) values ({version})
"
            });
        }

        catch (Exception e)
        {
            throw new Exception($"DsMigration.UpdateVersion() => {e.Message}");
        }
    }

    public void MigrateFrom3_0()
    {
        var tableNames = DataStore.GetTableNames(DataStore.Settings.SqlConnectionString);
        var indexScript = string.Empty;

        foreach (var tableName in tableNames)
        {
            indexScript += $@"
DROP INDEX IF EXISTS ix_json_data ON [dbo].[{tableName}]";
        }

        if (indexScript.StringHasValue())
        {
            try
            {
                _ = new SqlServerExecute(new SqlServerExecuteSettings
                {
                    SqlConnectionString = DataStore.Settings.SqlConnectionString,
                    CommandString = indexScript
                });
            }

            catch (Exception e)
            {
                throw new Exception($"DsMigration.MigrateFrom3_0() => {e.Message}");
            }
        }

        UpdateVersion(3.1);
    }

    public void PreMigrateFrom3_1()
    {
        var tableNames = DataStore.GetTableNames(DataStore.Settings.SqlConnectionString);
        var commands = new StringBuilder();

        // Drop functions and sprocs
        TaskTools.RunSynchronous(() => DataStore.DeleteDatabaseHelperObjectsAsync(DataStore.Settings.SqlConnectionString));

        // Rename tables as temp tables
        foreach (var tableName in tableNames)
        {
            if (string.IsNullOrEmpty(tableName) == false)
            {
                commands.Append($@"
ALTER TABLE [dbo].[{tableName}] DROP CONSTRAINT IF EXISTS DF_{tableName.Replace("datastore_", string.Empty)}_json_is_valid

EXEC sp_rename '{tableName}', '{tableName.Replace("datastore_", "datastoreTEMP_")}'
");
            }
        }

        if (commands.Length > 0)
        {
            try
            {
                _ = new SqlServerExecute(new SqlServerExecuteSettings
                {
                    SqlConnectionString = DataStore.Settings.SqlConnectionString,
                    CommandString = commands.ToString()
                });
            }

            catch (Exception e)
            {
                throw new Exception($"DsMigration.PreMigrateFrom3_1() => {e.Message}");
            }
        }
    }

    public void PreMigrateFrom3_2()
    {
        // Drop functions and sprocs
        TaskTools.RunSynchronous(() => DataStore.DeleteDatabaseHelperObjectsAsync(DataStore.Settings.SqlConnectionString));

        foreach (var modelType in ObjectTools.GetInheritedTypes(typeof(DsObject)).ToList())
        {
            var oldTableName = modelType.Name;
            var newTableName = DataStore.GenerateTableName(modelType);

            try
            {
                _ = new SqlServerExecute(new SqlServerExecuteSettings
                {
                    SqlConnectionString = DataStore.Settings.SqlConnectionString,
                    CommandString = $@"
ALTER TABLE [dbo].[datastore_{oldTableName}] DROP CONSTRAINT IF EXISTS DF_{oldTableName}_json_is_valid

EXEC sp_rename 'datastore_{oldTableName}', 'datastore_{newTableName}'
"
                });
            }

            catch (Exception e)
            {
                throw new Exception($"DsMigration.PreMigrateFrom3_2() => {e.Message}");
            }
        }
    }
    
    public void MigrateFrom3_1()
    {
        var tableNames = DataStore.GetTableNames(DataStore.Settings.SqlConnectionString);

        // Iterate temp content and save objects to new tables
        foreach (var tableName in tableNames)
        {
            if (string.IsNullOrEmpty(tableName) == false)
            {
                var tempTableName = tableName.Replace("datastore_", "datastoreTEMP_");

                using (var sqlConnection = new SqlConnection(DataStore.Settings.SqlConnectionString))
                {
                    using (var sqlCmd = new SqlCommand())
                    {
                        try
                        {
                            sqlCmd.CommandText = $"SELECT * FROM [dbo].[{tempTableName}]";
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
                                            var json = sqlDataReader.SqlSafeGetString("json_data");
                                            var jsonFragment =
$@"""Id"": {DataStore.JsonQuotedGuidOrNull(sqlDataReader.SqlSafeGetGuid("object_id"))},
""OwnerId"": {DataStore.JsonQuotedGuidOrNull(sqlDataReader.SqlSafeGetGuid("object_owner_id"))},
""ParentId"": {DataStore.JsonQuotedGuidOrNull(sqlDataReader.SqlSafeGetGuid("fk_object_parent_id"))},
""Lineage"": [{(sqlDataReader.SqlSafeGetString("object_lineage").StringHasValue() ? "\"" + sqlDataReader.SqlSafeGetString("object_lineage").Replace(",", "\",\"") + "\"" : string.Empty)}],
""Depth"": {sqlDataReader.SqlSafeGetInt("object_depth")},
""ObjectType"": {DataStore.JsonQuotedOrNull(sqlDataReader.SqlSafeGetString("object_type"))},
""CreateDate"": ""{sqlDataReader.SqlSafeGetDateTimeOffset("object_create_date"):o}"",
""LastDate"": ""{sqlDataReader.SqlSafeGetDateTimeOffset("object_last_date"):o}"",
""Sort"": {sqlDataReader.SqlSafeGetInt("object_sort")},
""IsDeleted"": {(sqlDataReader.SqlSafeGetInt("object_is_deleted") == 1 ? "true" : "false")},
";
                                            json = json.TrimStringStart("{")?.TrimStart();
                                            json = "{" + jsonFragment + json;

                                            var jsonDocument =
                                                JsonSerializer.Deserialize<dynamic>(json, DataStore.JsonSerializerOptions);

                                            json = JsonSerializer.Serialize(jsonDocument, DataStore.JsonSerializerOptions);

                                            try
                                            {
                                                _ = new SqlServerExecute(new SqlServerExecuteSettings
                                                {
                                                    SqlConnectionString = DataStore.Settings.SqlConnectionString,
                                                    CommandString = $"INSERT INTO [dbo].[{tableName}] ([json_data]) VALUES ({DataStore.QuotedOrNull(json.Replace("'", "''"))})" 
                                                });
                                            }

                                            catch (Exception e)
                                            {
                                                throw new Exception($"DsMigration.MigrateFrom3_1() => {e.Message}");
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

                try
                {
                    _ = new SqlServerExecute(new SqlServerExecuteSettings
                    {
                        SqlConnectionString = DataStore.Settings.SqlConnectionString,
                        CommandString = $"DROP TABLE [dbo].[{tempTableName}]" 
                    });
                }

                catch (Exception e)
                {
                    throw new Exception($"DsMigration.MigrateFrom3_1() => {e.Message}");
                }
            }
        }

        UpdateVersion(3.2);
    }
    
    #endregion
}
