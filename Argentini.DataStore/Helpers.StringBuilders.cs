using System.Text;

namespace Argentini.DataStore;

public static class StringBuilders
{
	/// <summary>
	/// Determines if a StringBuilder object has a value (is not null or empty).
	/// </summary>
	/// <param name="sb">String to evaluate</param>
	// ReSharper disable once MemberCanBePrivate.Global
	public static bool SbHasValue(this StringBuilder? sb)
	{
		return sb is {Length: > 0};
	}

	/// <summary>
	/// Determines if a StringBuilder is empty or null.
	/// </summary>
	/// <param name="sb"></param>
	public static bool SbIsEmpty(this StringBuilder sb)
	{
		return sb.SbHasValue() == false;
	}
	
	/// <summary>
	/// Get the index of the last occurrence of a substring, or -1 if not found
	/// </summary>
	/// <param name="source"></param>
	/// <param name="substring"></param>
	/// <param name="stringComparison"></param>
	/// <returns></returns>
	public static int SbLastIndexOf(this StringBuilder? source, string? substring, StringComparison stringComparison = StringComparison.Ordinal)
	{
		var result = -1;

		if (source != null && substring != null && source.SbHasValue() && substring.StringHasValue() && source.Length > substring.Length)
		{
			for (var x = source.Length - substring.Length - 1; x > -1; x--)
			{
				if (source.SbSubstring(x, substring.Length).Equals(substring, stringComparison))
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
	public static void SbTrimEnd(this StringBuilder source, string substring = " ", StringComparison stringComparison = StringComparison.Ordinal)
	{
		var index = source.SbLastIndexOf(substring, stringComparison);
	    
		while (index > -1)
		{
			source.Remove(index, substring.Length);
			index = source.SbLastIndexOf(substring, stringComparison);
		}
	}
	
	/// <summary>
	/// Clone a StringBuilder instance
	/// </summary>
	/// <param name="source"></param>
	/// <returns></returns>
	public static StringBuilder SbCreateClone(this StringBuilder source)
	{
		var maxCapacity = source.MaxCapacity;
		var capacity = source.Capacity;
		var newSb = new StringBuilder(capacity, maxCapacity);

		newSb.Append(source);

		return newSb;
	}

	/// <summary>
	/// Get a substring in a StringBuilder object.
	/// Exponentially faster than .ToString().SbSubstring().
	/// </summary>
	/// <param name="source">The source StringBuilder object</param>
	/// <param name="startIndex">A zero-based start index</param>
	/// <param name="length">String length to retrieve</param>
	/// <returns>SbSubstring or empty string if not found</returns>
	// ReSharper disable once MemberCanBePrivate.Global
	public static string SbSubstring(this StringBuilder source, int startIndex, int length)
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
	/// <param name="substring">SbSubstring to find</param>
	/// <param name="caseInsensitive">Ignore case if true</param>
	/// <returns>True is the StringBuilder object starts with the substring</returns>
	public static bool SbStartsWith(this StringBuilder source, string substring, bool caseInsensitive = false)
	{
		return (caseInsensitive ? SbSubstring(source, 0, substring.Length).ToUpper() : SbSubstring(source, 0, substring.Length)) == (caseInsensitive ? substring.ToUpper() : substring);
	}

	/// <summary>
	/// Determine if a StringBuilder object ends with a string.
	/// </summary>
	/// <param name="source">The StringBuilder object to evaluate</param>
	/// <param name="substring">SbSubstring to find</param>
	/// <param name="caseInsensitive">Ignore case if true</param>
	/// <returns>True is the StringBuilder object ends with the substring</returns>
	public static bool SbEndsWith(this StringBuilder source, string substring, bool caseInsensitive = false)
	{
		return (caseInsensitive ? SbSubstring(source, source.Length - substring.Length, substring.Length).ToUpper() : SbSubstring(source, source.Length - substring.Length, substring.Length)) == (caseInsensitive ? substring.ToUpper() : substring);
	}
}
