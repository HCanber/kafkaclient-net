using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Kafka.Client.Utils;

namespace Kafka.Client.IO
{
	/// <summary>
	/// Writes data into underlying stream using big endian bytes order for primitive types
	/// and UTF-8 encoding for strings.
	/// </summary>
	public class KafkaBinaryWriter : KafkaWriter
	{
		private readonly BinaryWriter _binaryWriter;
		private readonly CancellationToken _cancellationToken;
		internal int NumberOfWrittenBytes;

		/// <summary>
		/// Initializes a new instance of the <see cref="KafkaBinaryWriter"/> class 
		/// using big endian bytes order for primitive types and UTF-8 encoding for strings.
		/// </summary>
		protected KafkaBinaryWriter()
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="KafkaBinaryWriter"/> class 
		/// using big endian bytes order for primitive types and UTF-8 encoding for strings.
		/// </summary>
		/// <param name="output">
		///   The output stream.
		/// </param>
		/// <param name="cancellationToken"></param>
		public KafkaBinaryWriter(Stream output, CancellationToken cancellationToken)
			: this(new BinaryWriter(output),cancellationToken)
		{
		}


		/// <summary>
		/// Initializes a new instance of the <see cref="KafkaBinaryWriter"/> class 
		/// using big endian bytes order for primitive types and UTF-8 encoding for strings.
		/// </summary>
		/// <param name="output">
		///   The output stream.
		/// </param>
		public KafkaBinaryWriter(Stream output)
			: this(new BinaryWriter(output))
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="KafkaBinaryWriter"/> class 
		/// using big endian bytes order for primitive types and UTF-8 encoding for strings.
		/// </summary>
		public KafkaBinaryWriter(BinaryWriter binaryWriter)
		{
			_binaryWriter = binaryWriter;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="KafkaBinaryWriter"/> class 
		/// using big endian bytes order for primitive types and UTF-8 encoding for strings.
		/// </summary>
		public KafkaBinaryWriter(BinaryWriter binaryWriter, CancellationToken cancellationToken)
		{
			_binaryWriter = binaryWriter;
			_cancellationToken = cancellationToken;
			cancellationToken.ThrowIfCancellationRequested();
		}

		/// <summary>
		/// Writes one byte to the current stream 
		/// and advances the stream position by one byte
		/// </summary>
		public override void WriteByte(byte b)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			_binaryWriter.Write(b);
		}

		/// <summary>
		/// Writes two-bytes signed integer to the current stream using big endian bytes order 
		/// and advances the stream position by two bytes
		/// </summary>
		public override void WriteShort(short value)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			var bytes = BitConversion.GetBigEndianBytes(value);
			_binaryWriter.Write(bytes, 0, BitConversion.ShortSize);
			NumberOfWrittenBytes += BitConversion.ShortSize;
		}

		/// <summary>
		/// Writes four-bytes signed integer to the current stream using big endian bytes order 
		/// and advances the stream position by four bytes
		/// </summary>
		public override void WriteInt(int value)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			var bytes = BitConversion.GetBigEndianBytes(value);
			_binaryWriter.Write(bytes, 0, BitConversion.IntSize);
			NumberOfWrittenBytes += BitConversion.IntSize;
		}


		/// <summary>
		/// Writes four-bytes unsigned integer to the current stream using big endian bytes order 
		/// and advances the stream position by four bytes
		/// </summary>
		public void WriteUInt(uint value)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			var bytes = BitConversion.GetBigEndianBytes(value);
			_binaryWriter.Write(bytes, 0, BitConversion.IntSize);
			NumberOfWrittenBytes += BitConversion.IntSize;
		}
		/// <summary>
		/// Writes eight-bytes signed integer to the current stream using big endian bytes order 
		/// and advances the stream position by eight bytes
		/// </summary>
		public override void WriteLong(long value)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			var bytes = BitConversion.GetBigEndianBytes(value);
			_binaryWriter.Write(bytes, 0, BitConversion.LongSize);
			NumberOfWrittenBytes += BitConversion.LongSize;
		}

		/// <summary>
		/// Writes a string and its size into underlying stream using given encoding.
		/// </summary>
		/// <param name="value">
		/// The value to write.
		/// </param>
		public override void WriteShortString(string value)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			if(value == null)
			{
				WriteShort(-1);
			}
			else
			{
				var length = (short)value.Length;
				WriteShort(length);
				var encoding = Encoding.UTF8;

				var encodedString = encoding.GetBytes(value);
				_binaryWriter.Write(encodedString);
				NumberOfWrittenBytes += encodedString.Length;
			}
		}

		public override void WriteRepeated<T>(IReadOnlyCollection<T> items, Action<T> writeItem)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			var itemCount = items == null ? 0 : items.Count;
			WriteInt(itemCount);
			if(itemCount > 0)
			{
				foreach(var item in items)
				{
					writeItem(item);
				}
			}
		}
		public override void WriteRepeated<T>(IReadOnlyCollection<T> items, Action<T, int> writeItem)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			var itemCount = items == null ? 0 : items.Count;
			WriteInt(itemCount);
			if(itemCount > 0)
			{
				var index = 0;
				foreach(var item in items)
				{
					writeItem(item, index);
					index++;
				}
			}
		}
		public override void WriteRepeated<T>(IReadOnlyCollection<T> items, Action<KafkaWriter, T> writeItem)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			var itemCount = items == null ? 0 : items.Count;
			WriteInt(itemCount);
			if(itemCount > 0)
			{
				foreach(var item in items)
				{
					writeItem(this, item);
				}
			}
		}
		public override void WriteRepeated<T>(IReadOnlyCollection<T> items, Action<KafkaWriter, T, int> writeItem)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			var itemCount = items == null ? 0 : items.Count;
			WriteInt(itemCount);
			if(itemCount > 0)
			{
				var index = 0;
				foreach(var item in items)
				{
					writeItem(this, item, index);
					index++;
				}
			}
		}

		public override void WriteVariableBytes(byte[] bytes)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			if(bytes == null)
			{
				WriteInt(-1);
			}
			else
			{
				var length = bytes.Length;
				WriteInt(length);
				_binaryWriter.Write(bytes);
				NumberOfWrittenBytes += length;
			}
		}

		public override void WriteRaw(IRandomAccessReadBuffer buffer)
		{
			_cancellationToken.ThrowIfCancellationRequested();
			buffer.WriteTo(_binaryWriter.BaseStream);
			NumberOfWrittenBytes += buffer.Count;
		}

		public override void Dispose()
		{
			_binaryWriter.Flush();
			_binaryWriter.Dispose();
		}
		
	}
}