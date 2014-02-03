using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kafka.Client.Utils;

namespace Kafka.Client.IO
{
	public abstract class KafkaWriter : IDisposable
	{
		/// <summary>
		/// Flushes data into stream and resets position pointer.
		/// </summary>
		public abstract void Dispose();

		/// <summary>
		/// Writes four-bytes signed integer to the current stream using big endian bytes order 
		/// and advances the stream position by four bytes
		/// </summary>
		public abstract void WriteInt(int value);

		/// <summary>
		/// Writes eight-bytes signed integer to the current stream using big endian bytes order 
		/// and advances the stream position by eight bytes
		/// </summary>
		public abstract void WriteLong(long value);

		/// <summary>
		/// Writes two-bytes signed integer to the current stream using big endian bytes order 
		/// and advances the stream position by two bytes
		/// </summary>
		public abstract void WriteShort(short value);

		/// <summary>
		/// Writes a string and its size into underlying stream using given encoding.
		/// </summary>
		/// <param name="value">
		/// The value to write.
		/// </param>

		public abstract void WriteShortString(string value);

		public abstract void WriteArray<T>(IReadOnlyCollection<T> items, Action<T> writeItem);
		public abstract void WriteArray<T>(IReadOnlyCollection<T> items, Action<T, int> writeItem);
		public abstract void WriteArray<T>(IReadOnlyCollection<T> items, Action<KafkaWriter, T> writeItem);
		public abstract void WriteArray<T>(IReadOnlyCollection<T> items, Action<KafkaWriter, T, int> writeItem);

		public static int GetShortStringLength(string s)
		{
			if(s == null)
			{
				return BitConversion.ShortSize;
			}
			var byteCount = Encoding.UTF8.GetByteCount(s);
			return BitConversion.ShortSize + byteCount;
		}

		public static int GetArraySize<T>(IReadOnlyCollection<T> items, Func<T, int> calculateSizePerItem)
		{
			return BitConversion.IntSize + (items == null ? 0 : items.Sum(calculateSizePerItem));
		}
	}
}