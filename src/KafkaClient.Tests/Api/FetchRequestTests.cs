using Kafka.Client.Api;
using Kafka.Client.Utils;
using KafkaClient.Tests.TestHelpers;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class FetchRequestTests
	{
		[Fact]
		public void Given_single_request_Then_it_serializes_correctly()
		{
			var request = FetchRequest.CreateSingleRequest(topic: "test", partition: 42, offset: 4711, fetchSize: 100, minBytes: 123, maxWait: 456);
			var bytes = request.Serialize("client", 4711);

			var expectedSize = 2 + 2 + 4 + (2 + 6) +    //ApiKey + ApiVersion + CorrelationId + String_ClientId
												 4 + 4 + 4 + 4 +          //ReplicaId + MaxWaitTime + MinBytes + ArraySize_Topics
												 (2 + 4) + 4 + 4 + 8 + 4; // String_Topic + ArraySize_Partitions + Partition + FetchOffset + MaxBytes

			bytes.ShouldHaveLength(expectedSize);
			var index = 0;
			bytes.ShouldMatchRequestMessageHeader(ref index, 4711, "client", RequestApiKeys.Fetch);

			bytes.GetIntFromBigEndianBytes(16).ShouldBe(-1);	//ReplicaId=No node id
			bytes.GetIntFromBigEndianBytes(20).ShouldBe(456);	//MaxWaitTime
			bytes.GetIntFromBigEndianBytes(24).ShouldBe(123);	//MinBytes
			bytes.GetIntFromBigEndianBytes(28).ShouldBe(1);	  //Array size for Topics
			bytes.GetShortFromBigEndianBytes(32).ShouldBe<short>(4);	//Topic string length "test"
			bytes.ShouldBeString(34, "test");                   //Topic
			bytes.GetIntFromBigEndianBytes(38).ShouldBe(1);	    //Array size for Partitions
			bytes.GetIntFromBigEndianBytes(42).ShouldBe(42);	  //Partition
			bytes.GetLongFromBigEndianBytes(46).ShouldBe(4711); //Offset
			bytes.GetIntFromBigEndianBytes(54).ShouldBe(100);	  //MaxBytes, FetchSize
		}
	}
}