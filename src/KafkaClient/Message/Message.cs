using System;
using KafkaClient.IO;
using KafkaClient.Utils;

namespace KafkaClient.Message
{
	public class Message
	{
		private readonly IRandomAccessReadBuffer _buffer;
		//The current offset and size for all the fixed-length fields
		private const int _CrcOffset = 0;
		private const int _CrcLength = 4;
		private const int _MagicOffset = _CrcOffset + _CrcLength;
		private const int _MagicLength = 1;
		private const int _AttributesOffset = _MagicOffset + _MagicLength;
		private const int _AttributesLength = 1;
		private const int _KeySizeOffset = _AttributesOffset + _AttributesLength;
		private const int _KeySizeLength = 4;
		private const int _KeyOffset = _KeySizeOffset + _KeySizeLength;
		private const int _ValueSizeLength = 4;
	
	// The amount of overhead bytes in a message
		private const int _MessageOverhead = _KeyOffset + _ValueSizeLength;
		private const int _MinHeaderSize = _CrcLength + _MagicLength + _AttributesLength + _KeySizeLength + _ValueSizeLength;
	
	//The current "magic" value
		private const byte _CurrentMagicValue = 0;
	
	//Specifies the mask for the compression code. 2 bits to hold the compression codec.
	//0 is reserved to indicate no compression
		private const int _CompressionCodeMask = 0x03;

	
	//Compression code for uncompressed messages
		private const int _NoCompression = 0;


		private int _valueSizeOffset;

		private Message(IRandomAccessReadBuffer buffer)
		{
			_buffer = buffer;
			_valueSizeOffset = _KeyOffset + Math.Max(KeySize, 0);
		}

		public bool HasKey { get { return Key.Count > 0; } }

		public int KeySize { get { return _buffer.ReadInt(_KeySizeOffset); } }
		public ArraySegment<byte> Key { get { return _buffer.ReadByteArraySegment(_KeyOffset, KeySize); } }

		public int ValueSize { get { return _buffer.ReadInt(_valueSizeOffset); } }
		public ArraySegment<byte> Value { get { return _buffer.ReadByteArraySegment(_valueSizeOffset+BitConversion.IntSize, ValueSize); } }

		public int ComputeChecksum()
		{
			var segment = _buffer.GetAsArraySegment();
			var compute = Crc32.Compute(segment.Array, segment.Offset, segment.Count);
			throw new NotImplementedException();
		}

		public static Message Deserialize(IReadBuffer readBuffer, int messageSize)
		{
			return new Message(readBuffer.GetRandomAccessReadBuffer(messageSize));
		}
	}
}