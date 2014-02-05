using System;
using System.IO;
using System.Text;
using Kafka.Client.IO;
using Kafka.Client.Utils;

namespace Kafka.Client.Api
{
	public class Message : IMessage
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

		public Message(IRandomAccessReadBuffer buffer)
		{
			_buffer = buffer;
			_valueSizeOffset = _KeyOffset + Math.Max(KeySize, 0);
		}

		public Message(string value)
			: this((byte[])null, value == null ? null : Encoding.UTF8.GetBytes(value))
		{
		}

		public Message(byte[] value)
			: this((byte[])null, value)
		{
		}

		public Message(string key, string value) 
			: this(key == null ? null : Encoding.UTF8.GetBytes(key), value == null ? null : Encoding.UTF8.GetBytes(value)) 
		{ }

		public Message(string key, byte[] value) 
			: this(key == null ? null : Encoding.UTF8.GetBytes(key), value) 
		{ }

		public Message(byte[] key, byte[] value)
		{
			var size = CalculateMessageSize(key, value);
			var buffer = new byte[size];
			var stream = new MemoryStream(buffer);
			var writer = new KafkaBinaryWriter(stream);
			writer.WriteUInt(0);    //CRC, will receive it's correct value below
			writer.WriteByte(0);    //MagicByte
			writer.WriteByte(0);    //Attributes
			writer.WriteVariableBytes(key);
			writer.WriteVariableBytes(value);
			var crc = ComputeChecksum(buffer);
			stream.Seek(0, SeekOrigin.Begin);
			writer.WriteUInt(crc);     //CRC
			_buffer = new RandomAccessReadBuffer(buffer);
			_valueSizeOffset = _KeyOffset + Math.Max(KeySize, 0);
		}

		public bool HasKey { get { return KeySize >= 0; } }

		public int KeySize { get { return Math.Max(_buffer.ReadInt(_KeySizeOffset), -1); } }
		public ArraySegment<byte>? Key { get { return SliceOfSegment(_KeySizeOffset); } }

		public int ValueSize
		{
			get
			{
				var readInt = _buffer.ReadInt(_valueSizeOffset);
				return Math.Max(readInt, -1);
			}
		}

		public ArraySegment<byte>? Value { get { return SliceOfSegment(_valueSizeOffset); } }

		private ArraySegment<byte>? SliceOfSegment(int offset)
		{
			var size = _buffer.ReadInt(offset);

			return size < 0 ? (ArraySegment<byte>?)null : _buffer.ReadByteArraySegment(offset + BitConversion.IntSize, size);
		}

		public byte Magic { get { return _buffer.ReadByte(_MagicOffset); } }
		public byte Attributes { get { return _buffer.ReadByte(_AttributesOffset); } }
		public uint Checksum { get { return _buffer.ReadUInt(_CrcOffset); } }
		public bool IsValid { get { return Checksum == ComputeChecksum(); } }

		public uint ComputeChecksum()
		{
			var buffer = _buffer;
			return ComputeChecksum(buffer);
		}

		private static uint ComputeChecksum(IRandomAccessReadBuffer readBuffer)
		{
			var segment = readBuffer.GetAsArraySegment();
			var checksum = ComputeChecksum(segment.Array, segment.Offset, segment.Count);
			return checksum;
		}

		private static uint ComputeChecksum(byte[] buffer)
		{
			return ComputeChecksum(buffer, 0, buffer.Length);
		}

		private static uint ComputeChecksum(byte[] buffer, int offset, int length)
		{
			var checksum = Crc32.Compute(buffer, offset + BitConversion.IntSize, length - BitConversion.IntSize);
			return checksum;
		}

		int IKafkaRequestPart.GetSize()
		{
			return _buffer.Count;
		}

		void IKafkaRequestPart.WriteTo(KafkaWriter writer)
		{
			writer.WriteRaw(_buffer);
		}

		public static Message Deserialize(IReadBuffer readBuffer, int messageSize)
		{
			return new Message(readBuffer.GetRandomAccessReadBuffer(messageSize));
		}

		private static int CalculateMessageSize(byte[] key, byte[] value)
		{
			var keyLength = (key == null ? 0 : key.Length);
			var valueLength = (value == null ? 0 : value.Length);
			return BitConversion.IntSize +  //CRC
						 BitConversion.ByteSize + //Magic Byte
						 BitConversion.ByteSize + //Attributes
						 BitConversion.IntSize +  //Key Size
						 keyLength +              //Key #Bytes
						 BitConversion.IntSize +  //Value Size
						 valueLength;             //Value #Bytes
		}
	}
}