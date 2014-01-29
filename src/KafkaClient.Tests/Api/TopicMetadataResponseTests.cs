using System;
using KafkaClient.Api;
using KafkaClient.IO;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class TopicMetadataResponseTests
	{
		[Fact]
		public void Given_a_response_deserializes_correctly()
		{
			var bytes = new byte[]
			{
				0x01, 0x02, 0x03, 0x04,                  // Correlation Id
				0x00, 0x00, 0x00, 0x03,                  // # Brokers

				0x00, 0x00, 0x00, 1,                     // Broker id
				0x00, 0x04, (byte)'H', (byte)'o', (byte)'s', (byte)'t', // Host
				0x00, 0x00, 0x00, 21,                    // Port

				0x00, 0x00, 0x00, 2,                     // Broker id
				0x00, 0x04, (byte)'H', (byte)'o', (byte)'s', (byte)'t', // Host
				0x00, 0x00, 0x00, 22,                    // Port

				0x00, 0x00, 0x00, 3,                     // Broker id
				0x00, 0x04, (byte)'H', (byte)'o', (byte)'s', (byte)'t', // Host
				0x00, 0x00, 0x00, 23,                    // Port

				0x00, 0x00, 0x00, 0x01,                  // # Topic Meta datas

				0x00, 0x00,                                               //Error code
				0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t',   //Name
				0x00, 0x00, 0x00, 0x02,                                   //Error code

					0x00, 0x00,               //Error code
					0x00, 0x00, 0x45, 0x67,   //Partition Id
					0x00, 0x00, 0x00, 1,      //Leader
					0x00, 0x00, 0x00, 0x02,   // #Replicas
					0x00, 0x00, 0x00, 2,      // Replica1
					0x00, 0x00, 0x00, 3,      // Replica2
					0x00, 0x00, 0x00, 0x01,   // #InSyncReplicas
					0x00, 0x00, 0x00, 2,      // InSyncReplica1

					0x00, 0x00,               //Error code
					0x00, 0x00, 0x45, 0x68,   //Partition Id
					0x00, 0x00, 0x00, 2,      //Leader
					0x00, 0x00, 0x00, 0x02,   // #Replicas
					0x00, 0x00, 0x00, 1,      // Replica1
					0x00, 0x00, 0x00, 2,      // Replica2
					0x00, 0x00, 0x00, 0x00,   // #InSyncReplicas
			};
			var readBuffer = new ReadBuffer(bytes);
			var response = TopicMetadataResponse.Deserialize(readBuffer);
			readBuffer.BytesLeft.ShouldBe(0);
			response.CorrelationId.ShouldBe(0x01020304);
			response.BrokersById.ShouldHaveCount(3);
			response.BrokersById.ShouldOnlyContainKeys(1,2,3);
			response.BrokersById[1].Port.ShouldBe(21);
			response.TopicMetadatas.ShouldHaveCount(1);
		}
	}
}