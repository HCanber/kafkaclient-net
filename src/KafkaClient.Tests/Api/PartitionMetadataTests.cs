using System.Collections.Generic;
using KafkaClient.Api;
using KafkaClient.IO;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class PartitionMetadataTests
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
				0x00, 0x00,               //Error code
				0x00, 0x00, 0x45, 0x67,   //Partition Id
				0x00, 0x00, 0x00, 42,     //Leader
				0x00, 0x00, 0x00, 0x02,   // #Replicas
				0x00, 0x00, 0x00, 1,      // Replica1
				0x00, 0x00, 0x00, 11,     // Replica2
				0x00, 0x00, 0x00, 0x01,   // #InSyncReplicas
				0x00, 0x00, 0x00, 1,      // InSyncReplica1
			};
			var response = PartitionMetadata.Deserialize(new ReadBuffer(bytes),brokers);
			response.ErrorCode.ShouldBe<short>(0);
			response.Leader.ShouldBeSameAs(brokers[42]);
			response.PartitionId.ShouldBe(0x00004567);
			response.Replicas.ShouldHaveCount(2);
			response.Replicas.ShouldContain(brokers[1]);
			response.Replicas.ShouldContain(brokers[11]);
			response.InSyncReplicas.ShouldHaveCount(1);
			response.InSyncReplicas.ShouldContain(brokers[1]);
		}
	}
}