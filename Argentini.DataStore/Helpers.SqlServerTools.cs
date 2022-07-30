using System.Data;
using Microsoft.Data.SqlClient;

namespace Argentini.DataStore;

/// <summary>
/// Various tools to make using SqlConnection and SqlDataReader more bearable. 
/// </summary>
public static class SqlServerTools
{
	/// <summary>
	/// Determine if a column exists in an open SqlDataReader.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <returns></returns>
	public static bool SqlColumnExists(this SqlDataReader sqlDataReader, string columnName)
	{
		var result = false;

		try
		{
			if (columnName.StringHasValue())
			{
				if (sqlDataReader.IsClosed == false)
				{
					for (var i = 0; i < sqlDataReader.FieldCount; i++)
					{
						if (sqlDataReader.GetName(i).Equals(columnName, StringComparison.InvariantCultureIgnoreCase))
						{
							result = true;
							i = sqlDataReader.FieldCount;
						}
					}
				}
			}
		}

		catch
		{
			// ignored
		}

		return result;
	}

	/// <summary>
	/// Get the column index for a named column in an open SqlDataReader.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <returns></returns>
	public static int SqlColumnIndex(this SqlDataReader sqlDataReader, string columnName)
	{
		var result = -1;

		try
		{
			if (columnName.StringHasValue())
			{
				if (sqlDataReader.IsClosed == false)
				{
					for (var i = 0; i < sqlDataReader.FieldCount; i++)
					{
						if (sqlDataReader.GetName(i).Equals(columnName, StringComparison.InvariantCultureIgnoreCase))
						{
							result = i;
							i = sqlDataReader.FieldCount;
						}
					}
				}
			}
		}

		catch
		{
			// ignored
		}

		return result;
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<string> SqlSafeGetStringAsync(this SqlDataReader sqlDataReader, string columnName, string defaultValue = "")
	{
		if (sqlDataReader.SqlColumnExists(columnName))
		{
			return await sqlDataReader.IsDBNullAsync(columnName) ? defaultValue : sqlDataReader.GetString(columnName);
		}

		return defaultValue;
	}

	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static string SqlSafeGetString(this SqlDataReader sqlDataReader, string columnName, string defaultValue = "")
	{
		return TaskTools.RunSynchronous(() => SqlSafeGetStringAsync(sqlDataReader, columnName, defaultValue));
	}

	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<Guid> SqlSafeGetGuidAsync(this SqlDataReader sqlDataReader, string columnName, Guid defaultValue = new ())
	{
		if (sqlDataReader.SqlColumnExists(columnName))
		{
			return await sqlDataReader.IsDBNullAsync(columnName) ? defaultValue : sqlDataReader.GetGuid(columnName);
		}

		return defaultValue;
	}

	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static Guid SqlSafeGetGuid(this SqlDataReader sqlDataReader, string columnName, Guid defaultValue = new ())
	{
		return TaskTools.RunSynchronous(() => SqlSafeGetGuidAsync(sqlDataReader, columnName, defaultValue));
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<int> SqlSafeGetIntAsync(this SqlDataReader sqlDataReader, string columnName, int defaultValue = 0)
	{
		if (sqlDataReader.SqlColumnExists(columnName))
		{
			return await sqlDataReader.IsDBNullAsync(columnName) ? defaultValue : sqlDataReader.GetInt32(columnName);
		}

		return defaultValue;
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static int SqlSafeGetInt(this SqlDataReader sqlDataReader, string columnName, int defaultValue = 0)
	{
		return TaskTools.RunSynchronous(() => SqlSafeGetIntAsync(sqlDataReader, columnName, defaultValue));
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<long> SqlSafeGetLongAsync(this SqlDataReader sqlDataReader, string columnName, long defaultValue = 0)
	{
		if (sqlDataReader.SqlColumnExists(columnName))
		{
			return await sqlDataReader.IsDBNullAsync(columnName) ? defaultValue : sqlDataReader.GetInt64(columnName);
		}

		return defaultValue;
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static long SqlSafeGetLong(this SqlDataReader sqlDataReader, string columnName, long defaultValue = 0)
	{
		return TaskTools.RunSynchronous(() => SqlSafeGetLongAsync(sqlDataReader, columnName, defaultValue));
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<double> SqlSafeGetDoubleAsync(this SqlDataReader sqlDataReader, string columnName, double defaultValue = 0)
	{
		if (sqlDataReader.SqlColumnExists(columnName))
		{
			return await sqlDataReader.IsDBNullAsync(columnName) ? defaultValue : sqlDataReader.GetDouble(columnName);
		}

		return defaultValue;
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static double SqlSafeGetDouble(this SqlDataReader sqlDataReader, string columnName, double defaultValue = 0)
	{
		return TaskTools.RunSynchronous(() => SqlSafeGetDoubleAsync(sqlDataReader, columnName, defaultValue));
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<DateTimeOffset> SqlSafeGetDateTimeOffsetAsync(this SqlDataReader sqlDataReader, string columnName, DateTimeOffset defaultValue = new ())
	{
		var columnIndex = sqlDataReader.SqlColumnIndex(columnName);
		
		if (sqlDataReader.SqlColumnExists(columnName) && columnIndex > -1)
		{
			return await sqlDataReader.IsDBNullAsync(columnName) ? defaultValue : sqlDataReader.GetDateTimeOffset(columnIndex);
		}

		return defaultValue;
	}

	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static DateTimeOffset SqlSafeGetDateTimeOffset(this SqlDataReader sqlDataReader, string columnName, DateTimeOffset defaultValue = new ())
	{
		return TaskTools.RunSynchronous(() => SqlSafeGetDateTimeOffsetAsync(sqlDataReader, columnName, defaultValue));
	}
}

/// <summary>
/// Settings for the SqlServerExecute class.
/// </summary>
public class SqlServerExecuteSettings
{
	public string CommandString { get; init; } = string.Empty;
	public string SqlConnectionString { get; init; } = string.Empty;

	private readonly Dictionary<string, object> _parametersDictionary;
	public Dictionary<string, object> ParametersDictionary
	{
		get => _parametersDictionary;
		init => _parametersDictionary = value;
	}

	public int CommandTimeoutSeconds { get; init; }

	public SqlServerExecuteSettings()
	{
		_parametersDictionary = new Dictionary<string, object>();
	}
}

/// <summary>
/// Execute a T-SQL stored procedure or command text that has no return value.
/// Does not need to be disposed.
/// </summary>
public sealed class SqlServerExecute
{
	/// <summary>
	/// Instantiate the class and execute the T-SQL code.
	/// </summary>
	/// <example>
	/// <code>
	/// try
	/// {
	///     var execute = new SqlServerExecute(new SqlServerExecuteSettings
	///     {
	///         SqlConnectionString = sqlConnectionString,
	///         CommandString = commandText
	///     });
	/// }
	///   
	/// catch (Exception e)
	/// {
	///   	throw new Exception($"Uh oh => {e.Message}");
	/// }
	/// </code>
	/// </example>
	/// <param name="sqlServerExecuteSettings"></param>
	public SqlServerExecute(SqlServerExecuteSettings sqlServerExecuteSettings)
	{
		using (var sqlConnection = new SqlConnection(sqlServerExecuteSettings.SqlConnectionString))
		{
			using (var sqlCmd = new SqlCommand())
			{
				sqlCmd.CommandText = sqlServerExecuteSettings.CommandString;
				sqlCmd.Connection = sqlConnection;

				if (sqlServerExecuteSettings.ParametersDictionary.Any())
				{
					foreach (var (key, value) in sqlServerExecuteSettings.ParametersDictionary)
					{
						sqlCmd.Parameters.AddWithValue(key, value);
					}

					sqlCmd.CommandType = CommandType.StoredProcedure;
				}

				try
				{
					sqlConnection.Open();

					using (var sqlDataReader = sqlCmd.ExecuteReader())
					{
						sqlDataReader.Close();
					}
				}

				catch (Exception e)
				{
					if (sqlConnection.State != ConnectionState.Closed)
						sqlConnection.Close();

					throw new Exception($"Helpers.SqlServerExecute() => {e.Message}");
				}
			}

			sqlConnection.Close();
		}
	}
}
