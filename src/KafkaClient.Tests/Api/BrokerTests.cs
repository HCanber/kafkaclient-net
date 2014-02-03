using Kafka.Client.Api;
using Kafka.Client.IO;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class BrokerTests
	{
		[Fact]
		public void Given_a_response_deserializes_correctly()
		{
			var bytes = new byte[]
			{
				0x12, 0x34, 0x56, 0x78,                                  //NodeId
				0x00, 0x04,(byte) 't',(byte) 'e',(byte) 's',(byte) 't',  //Host
				0x00, 0x00, 0x22, 0x33,                                  //Port
			};
			var readBuffer = new ReadBuffer(bytes);
			var broker = Broker.Deserialize(readBuffer);
			readBuffer.BytesLeft.ShouldBe(0);
			broker.NodeId.ShouldBe(0x12345678);
			broker.Host.Host.ShouldBe("test");
			broker.Host.Port.ShouldBe<ushort>(0x2233);
		}
	}
}