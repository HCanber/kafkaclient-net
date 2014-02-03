using Kafka.Client.IO;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class MessageTests
	{
		[Fact]
		public void When_valid_message_Then_all_properties_are_correct()
		{
			var bytes = new byte[]
			{
				0x0F, 0x66, 0xBB, 0xB7,      // CRC
				0x00, 											 // MagicByte
				0x00, 											 // Attributes
				0xFF, 0xFF, 0xFF, 0xFF, 		 // Key, length = -1
				0x00, 0x00, 0x00, 0x03, 		 // Value, length
				0x48, 0x69, 0x21, 					 // Value "Hi!"
			};
			var buffer = new RandomAccessReadBuffer(bytes);
			var message = new global::Kafka.Client.Api.Message(buffer);
			message.HasKey.ShouldBeFalse();
			message.Key.Count.ShouldBe(0);
			message.ValueSize.ShouldBe(3);
			message.Value.Array[message.Value.Offset].ShouldBe((byte)'H');
			message.Value.Array[message.Value.Offset + 1].ShouldBe((byte)'i');
			message.Value.Array[message.Value.Offset + 2].ShouldBe((byte)'!');
			message.Magic.ShouldBe((byte)0);
			message.Attributes.ShouldBe((byte)0);
			message.Checksum.ShouldBe(0x0f66bbb7u);
			message.IsValid.ShouldBeTrue();
			message.ComputeChecksum().ShouldBe(0x0f66bbb7u);
		}

		[Fact]
		public void When_invalid_crc_Then_message_is_invalid()
		{
			var bytes = new byte[]
			{
				0xFF, 0xFF, 0xFF, 0xFF,      // CRC
				0x00, 											 // MagicByte
				0x00, 											 // Attributes
				0xFF, 0xFF, 0xFF, 0xFF, 		 // Key, length = -1
				0x00, 0x00, 0x00, 0x03, 		 // Value, length
				0x48, 0x69, 0x21, 					 // Value "Hi!"
			};
			var buffer = new RandomAccessReadBuffer(bytes);
			var message = new global::Kafka.Client.Api.Message(buffer);
			message.IsValid.ShouldBeFalse();
		}
	}
}