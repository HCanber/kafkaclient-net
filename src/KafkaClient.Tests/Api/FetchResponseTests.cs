using System;
using System.Linq;
using KafkaClient.Api;
using KafkaClient.IO;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class FetchResponseTests
	{
		[Fact]
		public void Given_a_response_deserialize_FetchRequest()
		{
			var bytes = new byte[]
			{
				0x12, 0x34, 0x56, 0x78,                          //CorrelationId
				0x00, 0x00, 0x00, 0x01,                          //Number of topics
				0x00, 0x01,                                      //  Topic, length
				0x54,                                            //  Topic "T"
				0x00, 0x00, 0x00, 0x01,                          //  Number of partitions
				0x01, 0x02, 0x03, 0x04,                          //    Partition
				0x00, 0x00,                                      //    ErrorCode=NoError
				0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04,  //    HighwaterMarkOffset
				0x00, 0x00, 0x00, 8+4+16+8+4+17,                 //    MessageSetSize
				0x11, 0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x24,  //       Offset
				0x00, 0x00, 0x00, 16,                            //       MessageSize
				0x01, 0x02, 0x03, 0x04,                          //       CRC
				0x00,                                            //       MagicByte
				0x00,                                            //       Attributes
				0x00, 0x00, 0x00, 0x01,                          //       Key, length
				0x4B,                                            //       Key "K"
				0x00, 0x00, 0x00, 0x01,                          //       Value, length
				0x56,                                            //       Value "V"

				0x12, 0x13, 0x14, 0x15, 0x26, 0x27, 0x28, 0x29,  //       Offset
				0x00, 0x00, 0x00, 17,                            //       MessageSize
				0x0F, 0x66, 0xBB, 0xB7,                          //       CRC
				0x00, 																					 //       MagicByte
				0x00, 																					 //       Attributes
				0xFF, 0xFF, 0xFF, 0xFF, 												 //       Key, length = -1
				0x00, 0x00, 0x00, 0x03, 												 //       Value, length
				0x48, 0x69, 0x21, 															 //       Value "Hi!"
			};
			var fetchResponse = FetchResponse.Deserialize(new ReadBuffer(bytes));
			fetchResponse.CorrelationId.ShouldBe(0x12345678);
			fetchResponse.HasError.ShouldBeFalse();
			fetchResponse.Data.ShouldHaveCount(1);
			var data = fetchResponse.Data.First();
			data.Key.ShouldBe(new TopicAndPartition("T", 0x01020304));
			data.Value.Error.ShouldBe(KafkaError.NoError);
			data.Value.ErrorValue.ShouldBe<short>(0);
			data.Value.HasError.ShouldBeFalse();
			data.Value.HighwaterMarkOffset.ShouldBe(0x0102030401020304);
			data.Value.Messages.ShouldHaveCount(2);
			var firstMessage = data.Value.Messages[0];
			firstMessage.Offset.ShouldBe(0x1112131421222324);
			firstMessage.Message.HasKey.ShouldBeTrue();
			firstMessage.Message.Key.Count.ShouldBe(1);
			firstMessage.Message.Key.Array[firstMessage.Message.Key.Offset].ShouldBe((byte)'K');
			firstMessage.Message.Value.Count.ShouldBe(1);
			firstMessage.Message.Value.Array[firstMessage.Message.Value.Offset].ShouldBe((byte)'V');
			var secondMessage = data.Value.Messages[1];
			secondMessage.Offset.ShouldBe(0x1213141526272829);
			secondMessage.Message.HasKey.ShouldBeFalse();
			secondMessage.Message.Key.Count.ShouldBe(0);
			secondMessage.Message.Value.Count.ShouldBe(3);
			secondMessage.Message.Value.Array[secondMessage.Message.Value.Offset].ShouldBe((byte)'H');
			secondMessage.Message.Value.Array[secondMessage.Message.Value.Offset + 1].ShouldBe((byte)'i');
			secondMessage.Message.Value.Array[secondMessage.Message.Value.Offset + 2].ShouldBe((byte)'!');
		}
	}
}