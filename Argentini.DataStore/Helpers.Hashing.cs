namespace Argentini.DataStore;

public static class Hashing
{
	/// <summary>
	/// Calculate the CRC-32 of a string.
	/// </summary>
	public static uint CalculateCrc32(this string payload)
	{
		return payload.StringToByteArray().CalculateCrc32();
	}

	/// <summary>
	/// Calculate the CRC-32 of a byte array.
	/// </summary>
	// ReSharper disable once MemberCanBePrivate.Global
	public static uint CalculateCrc32(this IEnumerable<byte> payload)
	{
		const uint sGenerator = 0xEDB88320;

		var mChecksumTable = Enumerable.Range(0, 256).Select(i =>
		{
			var tableEntry = (uint)i;

			for (var j = 0; j < 8; ++j)
			{
				tableEntry = ((tableEntry & 1) != 0)
					? (sGenerator ^ (tableEntry >> 1))
					: (tableEntry >> 1);
			}

			return tableEntry;

		}).ToArray();

		try
		{
			// Initialize checksumRegister to 0xFFFFFFFF and calculate the checksum.
			return ~payload.Aggregate(0xFFFFFFFF, (checksumRegister, currentByte) =>
				(mChecksumTable[(checksumRegister & 0xFF) ^ Convert.ToByte(currentByte)] ^ (checksumRegister >> 8)));
		}
		catch (FormatException e)
		{
			throw new Exception("Could not read the stream out as bytes.", e);
		}
		catch (InvalidCastException e)
		{
			throw new Exception("Could not read the stream out as bytes.", e);
		}
		catch (OverflowException e)
		{
			throw new Exception("Could not read the stream out as bytes.", e);
		}
	}
}
