using System;
using System.Collections.Generic;
using System.Text;
using KafkaClient.Utils;

namespace KafkaClient.IO
{
	public class ReadBuffer : IReadBuffer
	{
		private readonly byte[] _bytes;
		protected internal int Position;
		private int _count;
		private int _startIndex;


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
			var value = BitConversion.GetShortFromBigEndianBytes(_bytes, Position);
			Position += BitConversion.ShortSize;
			return value;
		}

		public int ReadInt()
		{
			var value = BitConversion.GetIntFromBigEndianBytes(_bytes, Position);
			Position += BitConversion.IntSize;
			return value;
		}

		public long ReadLong()
		{
			var value = BitConversion.GetLongFromBigEndianBytes(_bytes, Position);
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
	}
}