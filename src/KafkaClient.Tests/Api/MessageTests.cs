using System;
using System.IO;
using Kafka.Client.Api;
using Kafka.Client.IO;
using KafkaClient.Tests.TestHelpers;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class MessageTests
	{
		private readonly byte[] _testMessage1 = new byte[]
			{
				0x0F, 0x66, 0xBB, 0xB7,      // CRC
				0x00, 											 // MagicByte
				0x00, 											 // Attributes
				0xFF, 0xFF, 0xFF, 0xFF, 		 // Key, length = -1
				0x00, 0x00, 0x00, 0x03, 		 // Value, length
				0x48, 0x69, 0x21, 					 // Value "Hi!"
			};
		[Fact]
		public void When_valid_message_Then_all_properties_are_correct()
		{
			var bytes = _testMessage1;
			var buffer = new RandomAccessReadBuffer(bytes);
			var message = new global::Kafka.Client.Api.Message(buffer);
			message.HasKey.ShouldBeFalse();
			message.Key.ShouldNotHaveValue();
			message.KeySize.ShouldBe(-1);
			message.ValueSize.ShouldBe(3);
			message.Value.ShouldHaveValue();
			var arraySegment = message.Value.Value;
			arraySegment.Array[arraySegment.Offset].ShouldBe((byte)'H');
			arraySegment.Array[arraySegment.Offset + 1].ShouldBe((byte)'i');
			arraySegment.Array[arraySegment.Offset + 2].ShouldBe((byte)'!');
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

		[Fact]
		public void When_creating_message_with_No_key_and_A_Message_Then_all_properties_are_correctly_set()
		{
			var key = (string)null;
			var msg = new byte[] { 0x48, 0x69, 0x21 };	//Hi!

			var message = new Message(key, msg);
			message.HasKey.ShouldBeFalse();
			message.KeySize.ShouldBe(-1);
			message.Key.ShouldBeNull();

			message.ValueSize.ShouldBe(3);
			message.Value.ShouldHaveValue();

			var arraySegment = message.Value.Value;
			arraySegment.ShouldBeString("Hi!");
			message.Magic.ShouldBe((byte)0);
			message.Attributes.ShouldBe((byte)0);
			message.Checksum.ShouldBe(0x0f66bbb7u);
			message.IsValid.ShouldBeTrue();
			message.ComputeChecksum().ShouldBe(0x0f66bbb7u);

			((IKafkaRequestPart)message).GetSize().ShouldBe(17);
			var buffer = new byte[17];
			var writer = new KafkaBinaryWriter(new MemoryStream(buffer));
			((IKafkaRequestPart)message).WriteTo(writer);
			buffer.ShouldOnlyContainInOrder(_testMessage1);
		}

		[Fact]
		public void When_creating_message_with_A_key_and_A_Message_Then_all_properties_are_correctly_set()
		{
			var key = new byte[] { 0x4B };              //K
			var msg = new byte[] { 0x48, 0x69, 0x21 };  //Hi!

			var message = new Message(key, msg);
			message.HasKey.ShouldBeTrue();
			message.KeySize.ShouldBe(1);
			message.Key.ShouldHaveValue();
			message.Key.Value.ShouldBeString("K");

			message.ValueSize.ShouldBe(3);
			message.Value.ShouldHaveValue();
			message.Value.Value.ShouldBeString("Hi!");

			message.Magic.ShouldBe((byte)0);
			message.Attributes.ShouldBe((byte)0);
			message.Checksum.ShouldBe(0x265D0019u);
			message.IsValid.ShouldBeTrue();
			message.ComputeChecksum().ShouldBe(0x265D0019u);

			((IKafkaRequestPart)message).GetSize().ShouldBe(18);
			var buffer = new byte[18];
			var writer = new KafkaBinaryWriter(new MemoryStream(buffer));
			((IKafkaRequestPart)message).WriteTo(writer);

			var expectedBytes = new byte[]
			{
				0x26, 0x5D, 0x00, 0x19,      // CRC
				0x00, 											 // MagicByte
				0x00, 											 // Attributes
				0x00, 0x00, 0x00, 0x01, 		 // Key, length = -1
				0x4B,                        // Key "K"
				0x00, 0x00, 0x00, 0x03, 		 // Value, length
				0x48, 0x69, 0x21, 					 // Value "Hi!"
			};
			buffer.ShouldOnlyContainInOrder(expectedBytes);
		}

		[Fact]
		public void When_creating_message_with_A_key_and_No_Message_Then_all_properties_are_correctly_set()
		{
			var key = new byte[] { 0x4B };              //K
			var msg = (byte[])null;

			var message = new Message(key, msg);
			message.HasKey.ShouldBeTrue();
			message.KeySize.ShouldBe(1);
			message.Key.ShouldHaveValue();
			message.Key.Value.ShouldBeString("K");

			message.ValueSize.ShouldBe(-1);
			message.Value.ShouldBeNull();

	
			message.Magic.ShouldBe((byte)0);
			message.Attributes.ShouldBe((byte)0);
			message.Checksum.ShouldBe(0x51432BF2u);
			message.IsValid.ShouldBeTrue();
			message.ComputeChecksum().ShouldBe(0x51432BF2u);

			((IKafkaRequestPart)message).GetSize().ShouldBe(15);
			var buffer = new byte[15];
			var writer = new KafkaBinaryWriter(new MemoryStream(buffer));
			((IKafkaRequestPart)message).WriteTo(writer);

			var expectedBytes = new byte[]
			{
				0x51, 0x43, 0x2B, 0xF2,      // CRC
				0x00, 											 // MagicByte
				0x00, 											 // Attributes
				0x00, 0x00, 0x00, 0x01, 		 // Key, length
				0x4B,                        // Key "K"
				0xFF, 0xFF, 0xFF, 0xFF, 		 // Value, length = -1
			};
			buffer.ShouldOnlyContainInOrder(expectedBytes);
		}

		[Fact]
		public void When_creating_message_with_No_key_and_No_Message_Then_all_properties_are_correctly_set()
		{
			var key = (byte[])null; 
			var msg = (byte[])null;

			var message = new Message(key, msg);
			message.HasKey.ShouldBeFalse();
			message.KeySize.ShouldBe(-1);
			message.Key.ShouldBeNull();

			message.ValueSize.ShouldBe(-1);
			message.Value.ShouldBeNull();


			message.Magic.ShouldBe((byte)0);
			message.Attributes.ShouldBe((byte)0);
			message.Checksum.ShouldBe(0xA7EC6803u);
			message.IsValid.ShouldBeTrue();
			message.ComputeChecksum().ShouldBe(0xA7EC6803u);

			((IKafkaRequestPart)message).GetSize().ShouldBe(14);
			var buffer = new byte[14];
			var writer = new KafkaBinaryWriter(new MemoryStream(buffer));
			((IKafkaRequestPart)message).WriteTo(writer);

			var expectedBytes = new byte[]
			{
				0xA7, 0xEC, 0x68, 0x03,      // CRC
				0x00, 											 // MagicByte
				0x00, 											 // Attributes
				0xFF, 0xFF, 0xFF, 0xFF, 		 // Key, length = -1
				0xFF, 0xFF, 0xFF, 0xFF, 		 // Value, length = -1
			};
			buffer.ShouldOnlyContainInOrder(expectedBytes);
		}
	}
}