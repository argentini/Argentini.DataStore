using System.Text;

namespace Argentini.DataStore;

public static class StringBuilders
{
	/// <summary>
	/// Determines if a StringBuilder object has a value (is not null or empty).
	/// </summary>
	/// <param name="sb">String to evaluate</param>
	// ReSharper disable once MemberCanBePrivate.Global
	public static bool HasValue(this StringBuilder? sb)
	{
		return sb is {Length: > 0};
	}

	/// <summary>
	/// Determines if a StringBuilder is empty or null.
	/// </summary>
	/// <param name="sb"></param>
	public static bool IsEmpty(this StringBuilder sb)
	{
		return sb.HasValue() == false;
	}
	
	/// <summary>
	/// Get the index of the last occurrence of a substring, or -1 if not found
	/// </summary>
	/// <param name="source"></param>
	/// <param name="substring"></param>
	/// <param name="stringComparison"></param>
	/// <returns></returns>
	public static int LastIndexOf(this StringBuilder? source, string? substring, StringComparison stringComparison = StringComparison.Ordinal)
	{
		var result = -1;

		if (source != null && substring != null && source.HasValue() && substring.HasValue() && source.Length > substring.Length)
		{
			for (var x = source.Length - substring.Length - 1; x > -1; x--)
			{
				if (source.Substring(x, substring.Length).Equals(substring, stringComparison))
				{
					result = x;
					x = -1;
				}
			}
		}

		return result;
	}
	
	/// <summary>
	/// Remove a string from the end of a StringBuilder
	/// </summary>
	/// <param name="source">The StringBuilder to search</param>
	/// <param name="substring">The substring to remove</param>
	/// <returns>Trimmed source</returns>
	public static void TrimEnd(this StringBuilder source, string substring = " ", StringComparison stringComparison = StringComparison.Ordinal)
	{
		var index = source.LastIndexOf(substring, stringComparison);
	    
		while (index > -1)
		{
			source.Remove(index, substring.Length);
			index = source.LastIndexOf(substring, stringComparison);
		}
	}
	
	/// <summary>
	/// Clone a StringBuilder instance
	/// </summary>
	/// <param name="source"></param>
	/// <returns></returns>
	public static StringBuilder CreateClone(this StringBuilder source)
	{
		var maxCapacity = source.MaxCapacity;
		var capacity = source.Capacity;
		var newSb = new StringBuilder(capacity, maxCapacity);

		newSb.Append(source);

		return newSb;
	}

	/// <summary>
	/// Get a substring in a StringBuilder object.
	/// Exponentially faster than .ToString().Substring().
	/// </summary>
	/// <param name="source">The source StringBuilder object</param>
	/// <param name="startIndex">A zero-based start index</param>
	/// <param name="length">String length to retrieve</param>
	/// <returns>Substring or empty string if not found</returns>
	// ReSharper disable once MemberCanBePrivate.Global
	public static string Substring(this StringBuilder source, int startIndex, int length)
	{
		var result = string.Empty;

		if (source.Length <= 0) return result;
		if (startIndex < 0 || length <= 0) return result;
		if (startIndex + length > source.Length) return result;
		
		for (var x = startIndex; x < startIndex + length; x++)
		{
			result += source[x];
		}

		return result;
	}
	
	/// <summary>
	/// Determine if a StringBuilder object starts with a string.
	/// </summary>
	/// <param name="source">The StringBuilder object to evaluate</param>
	/// <param name="substring">Substring to find</param>
	/// <param name="caseInsensitive">Ignore case if true</param>
	/// <returns>True is the StringBuilder object starts with the substring</returns>
	public static bool StartsWith(this StringBuilder source, string substring, bool caseInsensitive = false)
	{
		return (caseInsensitive ? Substring(source, 0, substring.Length).ToUpper() : Substring(source, 0, substring.Length)) == (caseInsensitive ? substring.ToUpper() : substring);
	}

	/// <summary>
	/// Determine if a StringBuilder object ends with a string.
	/// </summary>
	/// <param name="source">The StringBuilder object to evaluate</param>
	/// <param name="substring">Substring to find</param>
	/// <param name="caseInsensitive">Ignore case if true</param>
	/// <returns>True is the StringBuilder object ends with the substring</returns>
	public static bool EndsWith(this StringBuilder source, string substring, bool caseInsensitive = false)
	{
		return (caseInsensitive ? Substring(source, source.Length - substring.Length, substring.Length).ToUpper() : Substring(source, source.Length - substring.Length, substring.Length)) == (caseInsensitive ? substring.ToUpper() : substring);
	}
}
