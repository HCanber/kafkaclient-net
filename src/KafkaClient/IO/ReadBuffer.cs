using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Client.Utils;

namespace Kafka.Client.IO
{
	public class ReadBuffer : IReadBuffer
	{
		private readonly byte[] _bytes;
		protected internal int Position;
		private readonly int _count;
		private readonly int _startIndex;


		public ReadBuffer(byte[] bytes, int startIndex = 0)
		{
			_bytes = bytes;
			_count = bytes.Length - startIndex;
			_startIndex = startIndex;
			Position = startIndex;
		}


		public int Count { get { return _count; } }
		public int CurrentPosition { get { return Position-_startIndex; } }
		public int BytesLeft { get { return Math.Max(_bytes.Length - Position,0); } }


		public byte ReadByte()
		{
			var value = _bytes[Position];
			Position += BitConversion.ByteSize;
			return value;
		}

		public short ReadShort()
		{
			var value = _bytes.GetShortFromBigEndianBytes(Position);
			Position += BitConversion.ShortSize;
			return value;
		}

		public int ReadInt()
		{
			var value = _bytes.GetIntFromBigEndianBytes(Position);
			Position += BitConversion.IntSize;
			return value;
		}

		public long ReadLong()
		{
			var value = _bytes.GetLongFromBigEndianBytes(Position);
			Position += BitConversion.LongSize;
			return value;
		}

		public string ReadShortString()
		{
			var size = ReadShort();
			var s = Encoding.UTF8.GetString(_bytes, Position, size);
			Position += size;
			return s;
		}

		public IReadOnlyList<T> ReadArray<T>(Func<IReadBuffer, T> parseItem)
		{
			var count = ReadInt();
			var items = new List<T>(count);
			for(int i = 0; i < count; i++)
			{
				var item = parseItem(this);
				items.Add(item);
			}
			return items;
		}

		public ArraySegment<byte> ReadByteArraySegment(int size)
		{
			if(size == -1) return EmptyArraySegment<byte>.Instance;
			var segment = new ArraySegment<byte>(_bytes, Position, size);
			Position += size;
			return segment;
		}

		public IRandomAccessReadBuffer GetRandomAccessReadBuffer(int size)
		{
			var segment = new RandomAccessReadBuffer(_bytes, Position, size);
			Position += size;
			return segment;
		}

		public IReadBuffer Slice(int size)
		{
			return new SlicedReadBuffer(this, size);
		}

		public void Skip(int numberOfBytes)
		{
			var bytesLeft = BytesLeft;
			if(numberOfBytes > bytesLeft)
			{
				throw new ArgumentOutOfRangeException("numberOfBytes", string.Format("The buffer only contains {0} bytes. Cannot skip {1} bytes", bytesLeft, numberOfBytes));
			}
			Position += numberOfBytes;
		}
	}
}