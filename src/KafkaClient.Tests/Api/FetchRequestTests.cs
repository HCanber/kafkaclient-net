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

			bytes.GetInt(ref index).ShouldBe(-1);	//ReplicaId=No node id
			bytes.GetInt(ref index).ShouldBe(456);	//MaxWaitTime
			bytes.GetInt(ref index).ShouldBe(123);	//MinBytes
			bytes.GetInt(ref index).ShouldBe(1);	  //Array size for Topics
			bytes.ShouldBeShortString(ref index, "test");                   //Topic
			bytes.GetInt(ref index).ShouldBe(1);	    //Array size for Partitions
			bytes.GetInt(ref index).ShouldBe(42);	  //Partition
			bytes.GetLong(ref index).ShouldBe(4711); //Offset
			bytes.GetInt(ref index).ShouldBe(100);	  //MaxBytes, FetchSize
		}
	}
}