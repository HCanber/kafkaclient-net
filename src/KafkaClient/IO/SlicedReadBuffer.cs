using System;
using System.Collections.Generic;
using Kafka.Client.Utils;

namespace Kafka.Client.IO
{
	public class SlicedReadBuffer : IReadBuffer
	{
		private readonly ReadBuffer _buffer;
		private readonly int _lastIndex;
		private int _count;
		private int _startIndex;

		public SlicedReadBuffer(ReadBuffer buffer, int size)
		{
			_buffer = buffer;
			var startIndex = buffer.Position;
			_count = size;
			_lastIndex = startIndex + size - 1;
			_startIndex = startIndex;
			if(_lastIndex>=buffer.Count)
				throw new ArgumentOutOfRangeException("size");
		}

		protected internal int Position { get { return _buffer.Position; } set { _buffer.Position = value; } }


		public int Count { get { return _count; } }
		public int CurrentPosition { get { return Position - _startIndex; } }
		public int BytesLeft { get { return Math.Max(_lastIndex + 1 - Position, 0); } }


		public byte ReadByte()
		{
			if(Position + BitConversion.ByteSize > _lastIndex+1)
				throw new IndexOutOfRangeException();
			return _buffer.ReadByte();
		}

		public short ReadShort()
		{
			if(Position + BitConversion.ShortSize > _lastIndex + 1)
				throw new IndexOutOfRangeException();
			return _buffer.ReadShort();
		}

		public int ReadInt()
		{
			if(Position + BitConversion.IntSize > _lastIndex + 1)
				throw new IndexOutOfRangeException();
			return _buffer.ReadInt();
		}

		public long ReadLong()
		{
			if(Position + BitConversion.LongSize > _lastIndex + 1)
				throw new IndexOutOfRangeException();
			return _buffer.ReadLong();
		}

		public string ReadShortString()
		{
			var positionBefore = Position;
			var s = _buffer.ReadShortString();
			if(Position > _lastIndex + 1)
			{
				Position = positionBefore;
				throw new IndexOutOfRangeException();
			}
			return s;
		}

		public IReadOnlyList<T> ReadArray<T>(Func<IReadBuffer, T> parseItem)
		{
			var positionBefore = Position;
			var list = _buffer.ReadArray(parseItem);
			if(Position > _lastIndex + 1)
			{
				Position = positionBefore;
				throw new IndexOutOfRangeException();
			}
			return list;
		}

		public ArraySegment<byte> ReadByteArraySegment(int size)
		{
			var positionBefore = Position;
			var segment = _buffer.ReadByteArraySegment(size);
			if(Position > _lastIndex + 1)
			{
				Position = positionBefore;
				throw new IndexOutOfRangeException();
			}
			return segment;
		}

		public IRandomAccessReadBuffer GetRandomAccessReadBuffer(int size)
		{
			if(Position + size > _lastIndex + 1)
				throw new IndexOutOfRangeException();
			var result = _buffer.GetRandomAccessReadBuffer(size);
			return result;
		}

		public IReadBuffer Slice(int size)
		{
			if(Position + size > _lastIndex + 1)
				throw new IndexOutOfRangeException();
			return new SlicedReadBuffer(_buffer, size);
		}

		public void Skip(int numberOfBytes)
		{
			if(Position + numberOfBytes > _lastIndex + 1)
				throw new IndexOutOfRangeException();
			_buffer.Skip(numberOfBytes);
		}
	}
}