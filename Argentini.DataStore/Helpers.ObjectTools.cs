using System.Collections;
using System.Reflection;
using System.Text;

namespace Argentini.DataStore;

public static class ObjectTools
{
	/// <summary>
	/// Get all types that inherit from a base type
	/// </summary>
	/// <param name="baseType"></param>
	/// <returns></returns>
	public static IEnumerable<Type> GetInheritedTypes(Type baseType, bool excludeSystemAssemblies = true)
	{
		var types = new List<Type>();
		
		foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies().Where(a => excludeSystemAssemblies == false || (excludeSystemAssemblies && (a.FullName?.StartsWith("System") ?? false) == false && (a.FullName?.StartsWith("Microsoft") ?? false) == false)))
		{
			var subtypes = assembly.GetTypes().Where(TheType => TheType.IsClass && TheType.IsAbstract == false).ToList();
			var ts = subtypes.Where(t => t != baseType && t.IsAssignableTo(baseType)).ToList();
			types.AddRange(ts);
		}
		
		return types;
	}

	/// <summary>
	/// Determine whether a type is a simple value-like type
	/// </summary>
	/// <param name="type">The type to check</param>
	/// <returns>A boolean indicating whether the type is a simple type or not</returns>
	// ReSharper disable once MemberCanBePrivate.Global
	public static bool IsSimpleDataType(this Type type)
	{
		return type.IsValueType || type.IsEnum || type == typeof(string);
	}
	
	/// <summary>
	/// Copy all fields and properties from a class object into those of a destination class object.
	/// Any properties or fields that are objects will use new object instances. 
	/// </summary>
	/// <param name="source">Clone this object's fields and properties</param>
	/// <param name="destination">Clone</param>
	public static void CloneObjectTo<T>(this T? source, T? destination)
	{
		TaskTools.WaitForTaskToComplete(CloneObjectToAsync(source, destination));		
	}
	
	/// <summary>
	/// Copy all fields and properties from a class object into those of a destination class object.
	/// Any properties or fields that are objects will use new object instances. 
	/// </summary>
	/// <param name="source">Clone this object's fields and properties</param>
	/// <param name="destination">Clone</param>
	public static async Task CloneObjectToAsync<T>(this T? source, T? destination)
	{
		if (source == null || destination == null) throw new Exception("Helpers.CloneObjectToAsync() => Source and destination cannot be null");
		
		if (typeof(T).IsSimpleDataType()) throw new Exception("Helpers.CloneObjectToAsync() => Cannot use on value types");

		if (typeof(T) == typeof(StringBuilder)) throw new Exception("Helpers.CloneObjectToAsync() => Cannot use on StringBuilder");

		await CloneProperties(source, destination);
	}
	
	private static async Task CloneProperties(object? source, object? destination)
	{
		if (source != null && destination != null)
		{
			foreach (var prop in source.GetType().GetFields())
			{
				if (prop.IsPublic)
				{
					await prop.ClonePropertyOrFieldValue(source, destination);
				}
			}

			foreach (var prop in source.GetType().GetProperties())
			{
				if ((prop.GetMethod?.IsPublic ?? false) && (prop.SetMethod?.IsPublic ?? false) && prop.CanRead && prop.CanWrite)
				{
					await prop.ClonePropertyOrFieldValue(source, destination);
				}
			}
		}
	}

	private static async Task ClonePropertyOrFieldValue(this MemberInfo propertyOrField, object source, object destination)
	{
		var propValue = propertyOrField.GetMemberValue(source);
		var valueType = propertyOrField.GetMemberPropertyOrFieldType();

		if (propValue != null)
		{
			if (valueType == typeof(StringBuilder))
			{
				propertyOrField.SetMemberValue(destination, (propValue as StringBuilder)?.SbCreateClone() ?? new StringBuilder());
			}

			else if (valueType.IsSimpleDataType())
			{
				propertyOrField.SetMemberValue(destination, propValue);
			}

			else if (valueType.IsClass)
			{
				if (valueType.GetInterfaces().Any(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
				{
					if (propValue is IList list)
					{
						if (valueType.BaseType == typeof(Array))
						{
							var arrayType = valueType.GetElementType();

							if (arrayType != null)
							{
								var newArray = Array.CreateInstance(arrayType, list.Count);
								var index = 0;

								foreach (var item in list)
								{
									if (arrayType.IsSimpleDataType())
									{
										newArray.SetValue(item, index++);
									}

									else
									{
										var subModel = Activator.CreateInstance(item.GetType());

										await CloneProperties(item, subModel);

										newArray.SetValue(subModel, index++);
									}
								}

								propertyOrField.SetMemberValue(destination, newArray);
							}
						}

						else
						{
							if (valueType.GetGenericArguments()[0] is { } enumerableType)
							{
								if (Activator.CreateInstance(valueType) is IList mpl)
								{
									foreach (var item in list)
									{
										if (enumerableType.IsSimpleDataType())
										{
											mpl.Add(item);
										}

										else
										{
											var subModel = Activator.CreateInstance(item.GetType());

											await CloneProperties(item, subModel);

											mpl.Add(subModel);
										}
									}

									propertyOrField.SetMemberValue(destination, mpl);
								}
							}
						}
					}

					else if (propValue is IDictionary dictionary)
					{
						if (dictionary.GetType() is { } dictType)
						{
							if (Activator.CreateInstance(dictType) is IDictionary newDict)
							{
								var kvpKeyType = newDict.GetType().GenericTypeArguments[0];
								var kvpValueType = newDict.GetType().GenericTypeArguments[1];
								
								foreach (var item in dictionary)
								{
									var kvp = new DictionaryEntry();

									if (kvpKeyType.IsSimpleDataType())
									{
										kvp.Key = ((DictionaryEntry) item).Key;
									}

									else
									{
										var subModel = Activator.CreateInstance(kvpKeyType);

										await CloneProperties(((DictionaryEntry) item).Key, subModel);

										kvp.Key = subModel!;
									}

									if (kvpValueType.IsSimpleDataType())
									{
										kvp.Value = ((DictionaryEntry) item).Value;
									}

									else
									{
										var subModel = Activator.CreateInstance(kvpValueType);

										await CloneProperties(((DictionaryEntry) item).Value, subModel);

										kvp.Value = subModel!;
									}
									
									newDict.Add(kvp.Key, kvp.Value);
								}

								propertyOrField.SetMemberValue(destination, newDict);
							}
						}
					}
				}

				else
				{
					var subModel = Activator.CreateInstance(valueType);

					await CloneProperties(propValue, subModel);

					propertyOrField.SetMemberValue(destination, subModel);
				}
			}
		}
	}
	
	public static object? GetMemberValue(this MemberInfo member, object srcObject)
	{
		return member switch
		{
			FieldInfo mfi => mfi.GetValue(srcObject),
			PropertyInfo mpi => mpi.GetValue(srcObject),
			MethodInfo mi => mi.Invoke(srcObject, null),
			_ => throw new ArgumentException("MemberInfo must be of type FieldInfo, PropertyInfo, or MethodInfo",
				nameof(member))
		};
	}
	public static T? GetMemberValue<T>(this MemberInfo member, object srcObject) => (T?)member.GetMemberValue(srcObject);

	public static void SetMemberValue(this MemberInfo member, object destObject, object? value)
	{
		switch (member) {
			case FieldInfo mfi:
				mfi.SetValue(destObject, value);
				break;
			case PropertyInfo mpi:
				mpi.SetValue(destObject, value);
				break;
			case MethodInfo mi:
				mi.Invoke(destObject, new[] { value });
				break;
			default:
				throw new ArgumentException("MemberInfo must be of type FieldInfo, PropertyInfo, or MethodInfo", nameof(member));
		}
	}
	public static void SetMemberValue<T>(this MemberInfo member, object destObject, T value) => member.SetMemberValue(destObject, (object?)value);	
	
	public static Type GetMemberPropertyOrFieldType(this MemberInfo member)
	{
		return member switch
		{
			FieldInfo mfi => mfi.FieldType,
			PropertyInfo mpi => mpi.PropertyType,
			EventInfo mei => mei.EventHandlerType,
			_ => throw new ArgumentException("MemberInfo must be of type FieldInfo, PropertyInfo, or EventInfo", nameof(member))
		} ?? typeof(object);
	}
	
	/// <summary>
	/// Copy all fields and properties from a class object into those of a destination class object.
	/// Any properties or fields that are objects will use new object instances. 
	/// </summary>
	/// <param name="source">Clone this object's fields and properties</param>
	/// <param name="duplicate">Clone</param>
	public static bool SameAsObject<T>(this T? source, T? duplicate)
	{
		var same = true;
		
		if (source == null && duplicate == null) return true;
		if (source == null && duplicate != null) return false;
		if (source != null && duplicate == null) return false;

		if (source != null && duplicate != null)
		{
			if (source.IsBasicType() && duplicate.IsBasicType())
			{
				same = CompareBasicTypes(source, duplicate, true);
			}

			else if (source.GetType().IsClass && duplicate.GetType().IsClass)
			{
				same = CompareProperties(source, duplicate, true);
			}

			else
			{
				same = false;
			}
		}

		return same;
	}

	private static bool CompareProperties(object? source, object? duplicate, bool same)
	{
		if (same == false) return false;
		if (source == null && duplicate == null) return true;
		if (source == null && duplicate != null) return false;
		if (source != null && duplicate == null) return false;

		var result = true;
		
		if (source != null && duplicate != null)
		{
			foreach (var prop in source.GetType().GetFields())
			{
				if (prop.IsPublic)
				{
					result = ComparePropertyOrFieldValues(prop, source, duplicate, result);
				}
			}

			foreach (var prop in source.GetType().GetProperties())
			{
				if ((prop.GetMethod?.IsPublic ?? false) && (prop.SetMethod?.IsPublic ?? false) && prop.CanRead && prop.CanWrite)
				{
					result = ComparePropertyOrFieldValues(prop, source, duplicate, result);
				}
			}
		}

		return result;
	}

	private static bool IsBasicType(this object? source)
	{
		if (source == null) return false;

		return IsBasicType(source.GetType());
	}

	private static bool IsBasicType(this Type valueType)
	{
		if (valueType.IsSimpleDataType() || valueType == typeof(DateTime) || valueType == typeof(DateTimeOffset) || valueType == typeof(StringBuilder))
		{
			return true;
		}
		
		return false;
	}
	
	private static bool CompareBasicTypes(object? source, object? duplicate, bool same)
	{
		if (same == false) return false;
		if (source == null && duplicate == null) return true;
		if (source == null && duplicate != null) return false;
		if (source != null && duplicate == null) return false;

		if (source != null && duplicate != null)
		{
			var valueType = source.GetType();
			
			if (valueType == typeof(string))
			{
				if (source is string sourceStr && duplicate is string duplicateStr)
					return sourceStr.Equals(duplicateStr, StringComparison.Ordinal);

				return false;
			}

			if (valueType == typeof(DateTime))
			{
				if (source is DateTime sourceDate && duplicate is DateTime duplicateDate)
					return sourceDate.Kind == duplicateDate.Kind && sourceDate.Ticks == duplicateDate.Ticks;

				return false;
			}

			if (valueType == typeof(DateTimeOffset))
			{
				if (source is DateTimeOffset sourceDate && duplicate is DateTimeOffset duplicateDate)
					return sourceDate.Offset.Ticks == duplicateDate.Offset.Ticks &&
					       sourceDate.UtcTicks == duplicateDate.UtcTicks;

				return false;
			}

			if (valueType.IsSimpleDataType()) return source.Equals(duplicate);

			if (valueType == typeof(StringBuilder))
			{
				if (source is StringBuilder sourceSb && duplicate is StringBuilder duplicateSb)
					return sourceSb.Capacity == duplicateSb.Capacity &&
					       sourceSb.MaxCapacity == duplicateSb.MaxCapacity && sourceSb.ToString()
						       .Equals(duplicateSb.ToString(), StringComparison.Ordinal);

				return false;
			}
		}
		
		return false;
	}
	
	private static bool ComparePropertyOrFieldValues(this MemberInfo propertyOrField, object? source, object? duplicate, bool same)
	{
		if (same == false) return false;
		if (source == null && duplicate == null) return true;
		if (source == null && duplicate != null) return false;
		if (source != null && duplicate == null) return false;

		var result = true;
		
		if (source != null && duplicate != null)
		{
			var sourceValue = propertyOrField.GetMemberValue(source);
			var duplicateValue = propertyOrField.GetMemberValue(duplicate);
			var valueType = propertyOrField.GetMemberPropertyOrFieldType();

			if (sourceValue == null && duplicateValue == null) return true;
			if (sourceValue == null && duplicateValue != null) return false;
			if (sourceValue != null && duplicateValue == null) return false;

			if (sourceValue != null && duplicateValue != null)
			{
				if (sourceValue.IsBasicType()) return CompareBasicTypes(sourceValue, duplicateValue, result);

				if (valueType.IsClass)
				{
					if (valueType.GetInterfaces().Any(t =>
						    t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
					{
						if (sourceValue is IList sourceList && duplicateValue is IList duplicateList)
						{
							if (sourceValue.GetType() != duplicateValue.GetType()) return false;

							if (sourceList.Count != duplicateList.Count) return false;
							
							if (valueType.BaseType == typeof(Array))
							{
								var arrayType = valueType.GetElementType();

								if (arrayType != null)
								{
									for (var index = 0; index < sourceList.Count; index++)
									{
										if (arrayType.IsBasicType())
										{
											result = CompareBasicTypes(sourceList[index], duplicateList[index], result);
										}

										else
										{
											result = CompareProperties(sourceList[index], duplicateList[index], result);
										}
									}
								}
							}

							else
							{
								if (valueType.GetGenericArguments()[0] is { } enumerableType)
								{
									for (var index = 0; index < sourceList.Count; index++)
									{
										if (enumerableType.IsBasicType())
										{
											result = CompareBasicTypes(sourceList[index], duplicateList[index], result);
										}

										else
										{
											result = CompareProperties(sourceList[index], duplicateList[index], result);
										}
									}
								}
							}
						}

						else if (sourceValue is IDictionary sourceDict && duplicateValue is IDictionary duplicateDict)
						{
							if (sourceValue.GetType() != duplicateValue.GetType()) return false;
							
							if (sourceDict.Count != duplicateDict.Count) return false;

							if (sourceDict.GetType() is { } _)
							{
								var kvpValueType = sourceDict.GetType().GenericTypeArguments[1];

								foreach (DictionaryEntry item in sourceDict)
								{
									if (kvpValueType.IsBasicType())
									{
										result = CompareBasicTypes(item.Value, duplicateDict[item.Key], result);
									}

									else
									{
										result = CompareProperties(item.Value, duplicateDict[item.Key], result);
									}
								}
							}
						}
					}

					else
					{
						result = CompareProperties(sourceValue, duplicateValue, result);
					}
				}
			}
		}

		return result;
	}
}
