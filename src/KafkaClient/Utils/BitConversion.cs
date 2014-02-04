using System;

namespace Kafka.Client.Utils
{
	/// <summary>
	/// Utilty class for managing bits and bytes.
	/// </summary>
	public static class BitConversion
	{
		private static readonly bool _SystemIsLittleEndian;
		public const int ByteSize = 1;
		public const int ShortSize = 2;
		public const int IntSize = 4;
		public const int LongSize = 8;

		static BitConversion()
		{
			_SystemIsLittleEndian = BitConverter.IsLittleEndian;
		}

		/// <summary>
		/// Converts the value to bytes in big endian order.
		/// </summary>
		/// <param name="value">The value to convert to bytes.</param>
		/// <returns>Bytes representing the value.</returns>
		public static byte[] GetBigEndianBytes(short value)
		{
			var bytes = BitConverter.GetBytes(value);
			if(_SystemIsLittleEndian)
				ReverseBytes(bytes, 0, ShortSize);
			return bytes;
		}

		/// <summary>
		/// Converts the value to bytes in big endian order.
		/// </summary>
		/// <param name="value">The value to convert to bytes.</param>
		/// <returns>Bytes representing the value.</returns>
		public static byte[] GetBigEndianBytes(int value)
		{
			var bytes = BitConverter.GetBytes(value);
			if(_SystemIsLittleEndian)
				ReverseBytes(bytes, 0, IntSize);
			return bytes;
		}

		/// <summary>
		/// Converts the value to bytes in big endian order.
		/// </summary>
		/// <param name="value">The value to convert to bytes.</param>
		/// <returns>Bytes representing the value.</returns>
		public static byte[] GetBigEndianBytes(uint value)
		{
			var bytes = BitConverter.GetBytes(value);
			if(_SystemIsLittleEndian)
				ReverseBytes(bytes, 0, IntSize);
			return bytes;
		}

		/// <summary>
		/// Converts the value to bytes in big endian order.
		/// </summary>
		/// <param name="value">The value to convert to bytes.</param>
		/// <returns>Bytes representing the value.</returns>
		public static byte[] GetBigEndianBytes(long value)
		{
			var bytes = BitConverter.GetBytes(value);
			if(_SystemIsLittleEndian)
				ReverseBytes(bytes, 0, LongSize);
			return bytes;
		}

		/// <summary>Gets the value as <see cref="short"/> from bytes in big endian order. </summary>
		public static short GetShortFromBigEndianBytes(this ArraySegment<byte> bytes, int startIndex = 0)
		{
			if(bytes.Count - startIndex < ShortSize) throw new ArgumentOutOfRangeException("startIndex", string.Format("Trying to read short from array at startIndex={0} when segment only contains {1} items", startIndex, bytes.Count));
			return GetShortFromBigEndianBytes(bytes.Array, startIndex + bytes.Offset);
		}

		/// <summary>Gets the value as <see cref="short"/> from bytes in big endian order. </summary>
		public static unsafe short GetShortFromBigEndianBytes(this byte[] bytes, int startIndex = 0)
		{
			fixed(byte* pbyte = &bytes[startIndex])
			{
				return (short)((*(pbyte + 1) << 0) | (*(pbyte + 0) << 8));
			}
		}


		/// <summary>Gets the value as <see cref="short"/> from bytes in big endian order. </summary>
		public static int GetIntFromBigEndianBytes(this ArraySegment<byte> bytes, int startIndex = 0)
		{
			if(bytes.Count - startIndex < IntSize) throw new ArgumentOutOfRangeException("startIndex", string.Format("Trying to read int from array at startIndex={0} when segment only contains {1} items", startIndex, bytes.Count));
			return GetIntFromBigEndianBytes(bytes.Array, startIndex + bytes.Offset);
		}

		/// <summary>Gets the value as <see cref="int"/> from bytes in big endian order.</summary>
		public static unsafe int GetIntFromBigEndianBytes(this byte[] bytes, int startIndex = 0)
		{
			fixed(byte* pbyte = &bytes[startIndex])
			{
				return (*(pbyte + 3) << 0) | (*(pbyte + 2) << 8) | (*(pbyte + 1) << 16) | (*(pbyte + 0) << 24);
			}
		}

		/// <summary>Gets the value as <see cref="int"/> from bytes in big endian order.</summary>
		public static unsafe uint GetUIntFromBigEndianBytes(this byte[] bytes, int startIndex = 0)
		{
			fixed(byte* pbyte = &bytes[startIndex])
			{
				return (((uint)(*(pbyte + 3))) << 0) | (((uint)(*(pbyte + 2))) << 8) | (((uint)(*(pbyte + 1))) << 16) | (((uint)(*(pbyte + 0))) << 24);
			}
		}


		/// <summary>Gets the value as <see cref="short"/> from bytes in big endian order. </summary>
		public static long GetLongFromBigEndianBytes(this ArraySegment<byte> bytes, int startIndex = 0)
		{
			if(bytes.Count - startIndex < LongSize) throw new ArgumentOutOfRangeException("startIndex", string.Format("Trying to read long from array at startIndex={0} when segment only contains {1} items", startIndex, bytes.Count));
			return GetLongFromBigEndianBytes(bytes.Array, startIndex + bytes.Offset);
		}

		/// <summary>
		/// Gets the value as <see cref="long"/> from bytes in big endian order.
		/// </summary>
		public static unsafe long GetLongFromBigEndianBytes(this byte[] bytes, int startIndex = 0)
		{
			fixed(byte* pbyte = &bytes[startIndex])
			{
				var i1 = (*(pbyte + 7) << 0) | (*(pbyte + 6) << 8) | (*(pbyte + 5) << 16) | (*(pbyte + 4) << 24);
				var i2 = (*(pbyte + 3) << 0) | (*(pbyte + 2) << 8) | (*(pbyte + 1) << 16) | (*(pbyte + 0) << 24);
				return (uint)i1 | ((long)i2 << 32);
			}
		}


		/// <summary>
		/// Reverse the position of an array of bytes.
		/// </summary>
		/// <param name="bytes">
		/// The array to reverse.
		/// </param>
		/// <returns>The reversed array.</returns>
		public static void ReverseBytes(byte[] bytes)
		{
			if(bytes == null) return;
			var length = bytes.Length;
			if(length == 0) return;

			ReverseBytes(bytes, 0, length);
		}

		/// <summary>
		/// Reverse the position of an array of bytes.
		/// </summary>
		/// <param name="inArray">
		/// The array to reverse.  If null or zero-length then the returned array will be null.
		/// </param>
		/// <param name="startIndex">The index to start on</param>
		/// <param name="length">Number of bytes to reverse. Must be a multiple of 2.</param>
		/// <returns>The reversed array.</returns>
		public static void ReverseBytes(byte[] inArray, int startIndex, int length)
		{
			if(inArray != null && inArray.Length > 0)
			{
				var highCtr = startIndex + length - 1;
				var middle = startIndex + (length / 2);

				for(var ctr = startIndex; ctr < middle; ctr++)
				{
					var temp = inArray[ctr];
					inArray[ctr] = inArray[highCtr];
					inArray[highCtr] = temp;
					highCtr -= 1;
				}
			}
		}
	}

}
