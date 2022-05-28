using System.Data;
using Microsoft.Data.SqlClient;

namespace Argentini.DataStore;

/// <summary>
/// Various tools to make using SqlConnection and SqlDataReader more bearable. 
/// </summary>
public static class SqlTools
{
	/// <summary>
	/// Determine if a column exists in an open SqlDataReader.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <returns></returns>
	public static bool ColumnExists(this SqlDataReader sqlDataReader, string columnName)
	{
		var result = false;

		try
		{
			if (columnName.HasValue())
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
	public static int ColumnIndex(this SqlDataReader sqlDataReader, string columnName)
	{
		var result = -1;

		try
		{
			if (columnName.HasValue())
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
	public static async Task<string> SafeGetStringAsync(this SqlDataReader sqlDataReader, string columnName, string defaultValue = "")
	{
		if (sqlDataReader.ColumnExists(columnName))
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
	public static string SafeGetString(this SqlDataReader sqlDataReader, string columnName, string defaultValue = "")
	{
		return Tasks.RunSync(() => SafeGetStringAsync(sqlDataReader, columnName, defaultValue));
	}

	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<Guid> SafeGetGuidAsync(this SqlDataReader sqlDataReader, string columnName, Guid defaultValue = new ())
	{
		if (sqlDataReader.ColumnExists(columnName))
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
	public static Guid SafeGetGuid(this SqlDataReader sqlDataReader, string columnName, Guid defaultValue = new ())
	{
		return Tasks.RunSync(() => SafeGetGuidAsync(sqlDataReader, columnName, defaultValue));
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<int> SafeGetIntAsync(this SqlDataReader sqlDataReader, string columnName, int defaultValue = 0)
	{
		if (sqlDataReader.ColumnExists(columnName))
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
	public static int SafeGetInt(this SqlDataReader sqlDataReader, string columnName, int defaultValue = 0)
	{
		return Tasks.RunSync(() => SafeGetIntAsync(sqlDataReader, columnName, defaultValue));
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<long> SafeGetLongAsync(this SqlDataReader sqlDataReader, string columnName, long defaultValue = 0)
	{
		if (sqlDataReader.ColumnExists(columnName))
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
	public static long SafeGetLong(this SqlDataReader sqlDataReader, string columnName, long defaultValue = 0)
	{
		return Tasks.RunSync(() => SafeGetLongAsync(sqlDataReader, columnName, defaultValue));
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<double> SafeGetDoubleAsync(this SqlDataReader sqlDataReader, string columnName, double defaultValue = 0)
	{
		if (sqlDataReader.ColumnExists(columnName))
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
	public static double SafeGetDouble(this SqlDataReader sqlDataReader, string columnName, double defaultValue = 0)
	{
		return Tasks.RunSync(() => SafeGetDoubleAsync(sqlDataReader, columnName, defaultValue));
	}
	
	/// <summary>
	/// Get a SqlDataReader column value or a default value if the column does not exist or is null.
	/// </summary>
	/// <param name="sqlDataReader"></param>
	/// <param name="columnName"></param>
	/// <param name="defaultValue"></param>
	/// <returns></returns>
	public static async Task<DateTimeOffset> SafeGetDateTimeOffsetAsync(this SqlDataReader sqlDataReader, string columnName, DateTimeOffset defaultValue = new ())
	{
		var columnIndex = sqlDataReader.ColumnIndex(columnName);
		
		if (sqlDataReader.ColumnExists(columnName) && columnIndex > -1)
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
	public static DateTimeOffset SafeGetDateTimeOffset(this SqlDataReader sqlDataReader, string columnName, DateTimeOffset defaultValue = new ())
	{
		return Tasks.RunSync(() => SafeGetDateTimeOffsetAsync(sqlDataReader, columnName, defaultValue));
	}
}

/// <summary>
/// Settings for the SqlExecute class.
/// </summary>
public class SqlExecuteSettings
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

	public SqlExecuteSettings()
	{
		_parametersDictionary = new Dictionary<string, object>();
	}
}

/// <summary>
/// Execute a T-SQL stored procedure or command text that has no return value.
/// Does not need to be disposed.
/// </summary>
public sealed class SqlExecute
{
	/// <summary>
	/// Instantiate the class and execute the T-SQL code.
	/// </summary>
	/// <example>
	/// <code>
	/// try
	/// {
	///     var execute = new SqlExecute(new SqlExecuteSettings
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
	/// <param name="sqlExecuteSettings"></param>
	public SqlExecute(SqlExecuteSettings sqlExecuteSettings)
	{
		using (var sqlConnection = new SqlConnection(sqlExecuteSettings.SqlConnectionString))
		{
			using (var sqlCmd = new SqlCommand())
			{
				sqlCmd.CommandText = sqlExecuteSettings.CommandString;
				sqlCmd.Connection = sqlConnection;

				if (sqlExecuteSettings.ParametersDictionary.Any())
				{
					foreach (var (key, value) in sqlExecuteSettings.ParametersDictionary)
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

					throw new Exception($"Helpers.SqlExecute() => {e.Message}");
				}
			}

			sqlConnection.Close();
		}
	}
}
