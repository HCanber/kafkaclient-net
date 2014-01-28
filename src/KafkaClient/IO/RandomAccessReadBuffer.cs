using System;
using System.Text;
using KafkaClient.Utils;

namespace KafkaClient.IO
{
	public class RandomAccessReadBuffer : IRandomAccessReadBuffer
	{
		private readonly byte[] _bytes;
		private readonly int _count;
		private readonly int _startIndex;


		public RandomAccessReadBuffer(byte[] bytes, int startIndex = 0)
			: this(bytes, startIndex, bytes.Length)
		{
		}

		public RandomAccessReadBuffer(byte[] bytes, int startIndex, int length)
		{
			_bytes = bytes;
			_count = length;
			_startIndex = startIndex;
		}
		public RandomAccessReadBuffer(ArraySegment<byte> segment)
		{
			_bytes = segment.Array;
			_count = segment.Count;
			_startIndex = segment.Offset;
		}


		public int Count { get { return _count; } }


		public byte ReadByte(int position)
		{
			if(position > _count) throw new ArgumentOutOfRangeException("position", string.Format("Trying to read a byte from position={0} when buffer only contains {1} items", position, _count));

			var value = _bytes[_startIndex + position];
			return value;
		}

		public short ReadShort(int position)
		{
			if(position + BitConversion.ShortSize > _count) throw new ArgumentOutOfRangeException("position", string.Format("Trying to read a short from position={0} when buffer only contains {1} items", position, _count));
			var value = _bytes.GetShortFromBigEndianBytes(_startIndex + position);
			return value;
		}

		public int ReadInt(int position)
		{
			if(position + BitConversion.IntSize > _count) throw new ArgumentOutOfRangeException("position", string.Format("Trying to read an int from position={0} when buffer only contains {1} items", position, _count));
			var value = _bytes.GetIntFromBigEndianBytes(_startIndex + position);
			return value;
		}

		public long ReadLong(int position)
		{
			if(position + BitConversion.LongSize > _count) throw new ArgumentOutOfRangeException("position", string.Format("Trying to read a long from position={0} when buffer only contains {1} items", position, _count));
			var value = _bytes.GetLongFromBigEndianBytes(_startIndex + position);
			return value;
		}

		public string ReadShortString(int position)
		{
			var size = ReadShort(position);
			if(size < 0) return null;
			if(size + BitConversion.ShortSize > _count) throw new ArgumentOutOfRangeException("position", string.Format("Trying to read a string of length {2} from position={0} when buffer only contains {1} items", position, _count, size));
			var s = Encoding.UTF8.GetString(_bytes, _startIndex + position + BitConversion.ShortSize, size);
			return s;
		}

		public ArraySegment<byte> GetAsArraySegment()
		{
			return new ArraySegment<byte>(_bytes, _startIndex ,_count);
		}

		public ArraySegment<byte> ReadByteArraySegment(int position, int size)
		{
			if(size <= 0) return EmptyArraySegment<byte>.Instance;
			if(size + position > _count) throw new ArgumentOutOfRangeException("position", string.Format("Trying to read an ArraySegement of length {2} from position={0} when buffer only contains {1} items", position, _count, size));
			var segment = new ArraySegment<byte>(_bytes, _startIndex + position, size);
			return segment;
		}

		public IRandomAccessReadBuffer Slice(int position, int size)
		{
			return new RandomAccessReadBuffer(_bytes, _startIndex + position, size);
		}
	}
}