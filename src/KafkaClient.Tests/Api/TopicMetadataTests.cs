using System;
using System.Collections.Generic;
using Kafka.Client.Api;
using Kafka.Client.IO;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class TopicMetadataTests
	{
		[Fact]
		public void Given_a_response_deserializes_correctly()
		{
			var brokers=new Dictionary<int, Broker>
			{
				{1,new Broker(1,"test",1)},
				{11,new Broker(11,"test",11)},
				{42,new Broker(42,"test",42)},
			};
			var bytes = new byte[]
			{
				0x00, 0x00,                                               //Error code
				0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t',   //Name
				0x00, 0x00, 0x00, 0x02,                                   //Error code

					0x00, 0x00,               //Error code
					0x00, 0x00, 0x45, 0x67,   //Partition Id
					0x00, 0x00, 0x00, 42,     //Leader
					0x00, 0x00, 0x00, 0x02,   // #Replicas
					0x00, 0x00, 0x00, 1,      // Replica1
					0x00, 0x00, 0x00, 11,     // Replica2
					0x00, 0x00, 0x00, 0x01,   // #InSyncReplicas
					0x00, 0x00, 0x00, 1,      // InSyncReplica1

					0x00, 0x00,               //Error code
					0x00, 0x00, 0x45, 0x68,   //Partition Id
					0x00, 0x00, 0x00, 11,     //Leader
					0x00, 0x00, 0x00, 0x02,   // #Replicas
					0x00, 0x00, 0x00, 42,     // Replica1
					0x00, 0x00, 0x00, 1,      // Replica2
					0x00, 0x00, 0x00, 0x00,   // #InSyncReplicas
			};
			var readBuffer = new ReadBuffer(bytes);
			var response = TopicMetadata.Deserialize(readBuffer,brokers);
			readBuffer.BytesLeft.ShouldBe(0);
			response.Topic.ShouldBe("test");
			response.ErrorCode.ShouldBe<short>(0);
			response.PartionMetaDatas.ShouldHaveCount(2);
		}
	}
}