using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace Argentini.DataStore;

public static class Strings
{
	/// <summary>
	/// RepeatString the first character a given string up to a specified number.
	/// </summary>
	/// <param name="text">String with first character to repeat</param>
	/// <param name="width">Width in characters of the final string</param>
	/// <returns>Text repeated up to the given width</returns>
	public static string RepeatString(this string text, int width)
	{
		var result = string.Empty;

		if (width > 0)
		{
			if (text.StringHasValue())
			{
				for (int x = 0; x < width; x++)
				{
					result += text.Substring(0, 1);
				}
			}
		}

		return result;
	}
	
	/// <summary>
	/// Convert unicode characters with diacritic marks into English equivalents.
	/// </summary>
	/// <param name="text">String to evaluate</param>
	/// <returns></returns>
	// ReSharper disable once MemberCanBePrivate.Global
	public static string RemoveStringDiacritics(this string text)
	{
		if (string.IsNullOrWhiteSpace(text))
			return text;

		text = text.Normalize(NormalizationForm.FormD);
		var chars = text.Where(c => CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark).ToArray();

		return new string(chars).Normalize(NormalizationForm.FormC);
	}
	
	/// <summary>
	/// Convert a string into a URL-friendly slug, filtering out everything but alphanumeric characters
	/// and using hyphens for whitespace.
	/// </summary>
	/// <param name="input">String to evaluate</param>
	/// <param name="allow">String of characters to allow</param>
	/// <returns>URL-friendly slug</returns>
	public static string ToSlugString(this string input, string allow = "")
	{
		var result = string.Empty;

		if (!input.StringHasValue()) return (result);

		var expression = "([^a-zA-Z0-9])";

		if (allow.StringHasValue()) expression = "([^a-zA-Z0-9" + Regex.Escape(allow) + "])";

		result = input.Trim().ToLower().RemoveStringDiacritics();
		result = result.Replace(" & ", " and ");
		result = result.Replace("'" , string.Empty);

		var stripStuff = new Regex(expression);
        
		result = stripStuff.Replace(result, "-");

		while (result.IndexOf("--", StringComparison.Ordinal) > 0)
		{
			result = result.Replace("--", "-");
		}

		result = result.Trim(new[] { '-' });

		return (result);
	}
	
	/// <summary>
	/// Take first, middle, and last name and makes a sortable string as Last, First Middle
	/// </summary>
	/// <param name="firstName">First name</param>
	/// <param name="middleName">Middle name</param>
	/// <param name="lastName">Last name</param>
	/// <returns>Sortable name</returns>
	public static string SortableNameString(string firstName, string middleName, string lastName)
	{
		var result = string.Empty;

		if (firstName.StringHasValue() || middleName.StringHasValue() || lastName.StringHasValue())
		{
			result = ((lastName.StringHasValue() ? lastName.Trim() + "," : string.Empty) + (firstName.StringHasValue() ? " " + firstName.Trim() : string.Empty) + (middleName.StringHasValue() ? " " + middleName.Trim() : string.Empty)).Trim(new char[] { ' ', ',' });
		}

		return result;
	}
	
	/// <summary>
	/// Convert a string to a byte array.
	/// </summary>
	/// <param name="value">String to evaluate</param>
	/// <returns>Byte array</returns>
	public static IEnumerable<byte> StringToByteArray(this string value)
	{
		var encoding = new UTF8Encoding();

		return encoding.GetBytes(value);
	}
	
	/// <summary>
	/// Determines if a string has a value (is not null and not empty).
	/// </summary>
	/// <param name="value">String to evaluate</param>
	public static bool StringHasValue(this string? value)
	{
		value = value?.Trim();

		return (string.IsNullOrEmpty(value) == false);
	}

	/// <summary>
	/// Determines if a string is empty or null.
	/// </summary>
	/// <param name="value">String to evaluate</param>
	public static bool StringIsEmpty(this string? value)
	{
		return string.IsNullOrEmpty(value);
	}

	/// <summary>
	/// Normalize line breaks.
	/// </summary>
	/// <param name="value"></param>
	/// <param name="linebreak">Line break to use (default: "\n")</param>
	public static string NormalizeStringLinebreaks(this string value, string linebreak = "\n")
	{
		var result = value;

		if (!value.StringHasValue()) return result;
        
		if (value.Contains("\r\n") && linebreak != "\r\n")
		{
			result = value.Replace("\r\n", linebreak);
		}

		else if (value.Contains('\r') && linebreak != "\r")
		{
			result = value.Replace("\r", linebreak);
		}

		else if (value.Contains('\n') && linebreak != "\n")
		{
			result = value.Replace("\n", linebreak);
		}

		return result;
	}

	/// <summary>
	/// Remove a specified number of characters from the beginning of a string
	/// </summary>
	/// <param name="value">String to trim</param>
	/// <param name="count">Number of characters to remove</param>
	/// <returns>Trimmed string</returns>
	public static string? TrimStringStart(this string? value, int count)
	{
		if (value != null && value.Length >= count)
		{
			value = value.StringRight(value.Length - count);
		}

		return value;
	}

	/// <summary>
	/// Remove a specified string from the beginning of a string
	/// </summary>
	/// <param name="value">String to trim</param>
	/// <param name="remove">String to remove</param>
	/// <returns>Trimmed string</returns>
	public static string? TrimStringStart(this string? value, string remove)
	{
		if (value != null && value.Length < remove.Length) return value;
		
		if (value != null && value.StartsWith(remove))
		{
			value = value.StringRight(value.Length - remove.Length);
		}

		return value;
	}

	/// <summary>
	/// Remove a specified number of characters from the end of a string
	/// </summary>
	/// <param name="value">String to trim</param>
	/// <param name="count">Number of characters to remove</param>
	/// <returns>Trimmed string</returns>
	public static string? TrimStringEnd(this string? value, int count)
	{
		if (value != null && value.Length >= count)
		{
			value = value.StringLeft(value.Length - count);
		}

		return value;
	}

	/// <summary>
	/// Remove a specified string from the end of a string
	/// </summary>
	/// <param name="value">String to trim</param>
	/// <param name="remove">String to remove</param>
	/// <returns>Trimmed string</returns>
	public static string? TrimStringEnd(this string? value, string remove)
	{
		if (value == null || value.Length < remove.Length) return value;
		
		if (value.EndsWith(remove))
		{
			value = value.StringLeft(value.Length - remove.Length);
		}

		return value;
	}

	/// <summary>
	/// Get the left "length" characters of a string.
	/// </summary>
	/// <param name="value">String value</param>
	/// <param name="length">Number of characters</param>
	/// <returns>StringLeft portion of a string</returns>
	public static string? StringLeft(this string? value, int length)
	{
		var result = value;

		if (value != null)
		{
			if (value.StringIsEmpty()) return result;

			if (value.Length >= length)
			{
				result = value[..length];
			}
		}

		return result;
	}

	/// <summary>
	/// Get the left characters of a string up to but not including the first instance of "marker".
	/// </summary>
	/// <param name="value">String value</param>
	/// <param name="marker">Delimiter to denote the cut off point</param>
	/// <returns>StringLeft portion of a string</returns>
	public static string? StringLeft(this string? value, string marker)
	{
		var result = value;

		if (value != null)
		{
			if (value.StringIsEmpty() || marker.StringIsEmpty()) return result;

			if (value.Length < marker.Length) return result;

			if (value.Contains(marker))
			{
				result = value[..value.IndexOf(marker, StringComparison.Ordinal)];
			}
		}

		return result;
	}

	/// <summary>
	/// Get the left characters of a string up to but not including the first instance of "marker".
	/// </summary>
	/// <param name="value">String value</param>
	/// <param name="marker">Delimiter to denote the cut off point</param>
	/// <returns>StringLeft portion of a string</returns>
	public static string? StringLeft(this string? value, char marker)
	{
		var result = value;

		if (value != null && value.StringHasValue())
		{
			if (value.Length < 1) return result;

			if (value.Contains(marker))
			{
				result = value[..value.IndexOf(marker, StringComparison.Ordinal)];
			}
		}

		return result;
	}
	
	/// <summary>
	/// Get the right "length" characters of a string.
	/// </summary>
	/// <param name="value">String value</param>
	/// <param name="length">Number of characters</param>
	/// <returns>StringRight portion of a string</returns>
	public static string? StringRight(this string? value, int length)
	{
		var result = value;

		if (value != null)
		{
			if (value.StringIsEmpty()) return result;

			if (value.Length >= length)
			{
				result = value[^length..];
			}
		}

		return result;
	}

	/// <summary>
	/// Get the right characters of a string up to but not including the last instance of "marker" (right to left).
	/// </summary>
	/// <param name="value">String value</param>
	/// <param name="marker">Delimiter to denote the cut off point</param>
	/// <returns>StringRight portion of a string</returns>
	public static string? StringRight(this string? value, string marker)
	{
		var result = value;

		if (value != null)
		{
			if (value.StringIsEmpty() || marker.StringIsEmpty()) return result;

			if (value.Length < marker.Length) return result;

			if (value.Contains(marker))
			{
				result = value[(value.LastIndexOf(marker, StringComparison.Ordinal) + marker.Length)..];
			}
		}

		return result;
	}

	/// <summary>
	/// Get the right characters of a string up to but not including the last instance of "marker" (right to left).
	/// </summary>
	/// <param name="value">String value</param>
	/// <param name="marker">Delimiter to denote the cut off point</param>
	/// <returns>StringRight portion of a string</returns>
	public static string? StringRight(this string? value, char marker)
	{
		var result = value;

		if (value != null && value.StringHasValue())
		{
			if (value.Length < 1) return result;

			if (value.Contains(marker))
			{
				result = value[(value.LastIndexOf(marker.ToString(), StringComparison.Ordinal) + 1)..];
			}
		}

		return result;
	}
	
	/// <summary>
	/// StringIndent text with given whitespace based on line breaks
	/// </summary>
	/// <param name="block"></param>
	/// <param name="whitespace"></param>
	/// <param name="includeLeading"></param>
	/// <returns></returns>
	public static string StringIndent(this string block, string whitespace, bool includeLeading = false)
	{
		var result = block.Trim().NormalizeStringLinebreaks("\r\n");
        
		if (result.StringHasValue())
		{
			result = result.Replace("\r\n", "\r\n" + whitespace);
		}

		return (includeLeading ? whitespace : string.Empty) + result.Trim();
	}
	
	/// <summary>
	/// <![CDATA[
	/// Sanitize a string so that it resists SQL injection attacks;
	/// replaces single apostrophes with two apostrophes.
	/// ]]>
	/// </summary>
	/// <param name="value">String to sanitize</param>
	/// <returns>A sanitized string.</returns>
	public static string SqlSanitizeString(this string value)
	{
		return value.Replace("'", "''");
	}
	
	#region Timers
	
	/// <summary>
	/// Format the elapsed time as a more friendly time span with a custom delimiter.
	/// Like: 3d : 5h : 12m : 15s or 3d+5h+12m+15s
	/// </summary>
	/// <param name="delimiter">Text to separate time elements; defaults to " : "</param>
	/// <returns>Formatted timespan</returns>
	public static string FormatTimerString(double msecs)
	{
		var timespan = TimeSpan.FromMilliseconds(msecs);
		return $"{(timespan.Days > 0 ? timespan.Days.ToString("#,##0") + " days " : "")}{timespan.Hours:00}:{timespan.Minutes:00}:{timespan.Seconds:00}.{timespan.Milliseconds:#000}";
	}

	/// <summary>
	/// Returns a string with the time in seconds as well as the performance per second
	/// (e.g. "100.2 sec (10,435.1/sec)")
	/// </summary>
	/// <param name="numberProcessed">Number of items processed in the elapsed time</param>
	/// <param name="msecs">Number milliseconds to output (overrides ElapsedMs)</param>
	/// <param name="decimalPlaces">Number of decimal places to show</param>
	/// <returns></returns>
	public static string PerformanceTime(int numberProcessed, double msecs, int decimalPlaces = 1)
	{
		return $"{FormatTimerString(msecs)} ({Performance(numberProcessed, msecs, decimalPlaces)})";
	}

	/// <summary>
	/// Returns a string with the performance per second
	/// (e.g. "10,435.1/sec")
	/// </summary>
	/// <param name="numberProcessed">Number of items processed in the elapsed time</param>
	/// <param name="msecs">Number milliseconds to output (overrides ElapsedMs)</param>
	/// <param name="decimalPlaces">Number of decimal places to show</param>
	/// <returns></returns>
	public static string Performance(int numberProcessed, double msecs, int decimalPlaces = 1)
	{
		var secs = msecs / 1000;

		return $"{Math.Round(numberProcessed / secs, decimalPlaces).ToString($"N{decimalPlaces}")}/sec";
	}
	
	#endregion
}
