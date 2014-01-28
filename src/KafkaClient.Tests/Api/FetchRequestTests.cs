using System;
using KafkaClient.Api;
using KafkaClient.Tests.TestHelpers;
using KafkaClient.Utils;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class FetchRequestTests
	{
		[Fact]
		public void Given_single_request_Then_Write_writes_correct()
		{
			var request = FetchRequest.CreateSingleRequest(topic: "test", partition: 42, offset: 4711, fetchSize: 100, clientId: "client", minBytes: 123, maxWait: 456, correlationId: 789);
			var bytes = request.Write();

			var expectedSize = 2 + 2 + 4 + (2 + 6) +    //ApiKey + ApiVersion + CorrelationId + String_ClientId
												 4 + 4 + 4 + 4 +          //ReplicaId + MaxWaitTime + MinBytes + ArraySize_Topics
												 (2 + 4) + 4 + 4 + 8 + 4; // String_Topic + ArraySize_Partitions + Partition + FetchOffset + MaxBytes

			var expectedTotalMessageLength = expectedSize + 4;
			bytes.ShouldHaveLength(expectedTotalMessageLength);

			bytes.GetIntFromBigEndianBytes(0).ShouldBe(expectedSize);	//Size
			bytes.GetShortFromBigEndianBytes(4).ShouldBe<short>(1);	//ApiKey=FetchRequest
			bytes.GetShortFromBigEndianBytes(6).ShouldBe<short>(0);	//ApiVersion
			bytes.GetIntFromBigEndianBytes(8).ShouldBe(789);	//CorrelationId
			bytes.GetShortFromBigEndianBytes(12).ShouldBe<short>((short)"client".Length);	//ClientId string length
			bytes.ShouldBeString(14, "client");                       //ClientId

			bytes.GetIntFromBigEndianBytes(20).ShouldBe(-1);	//ReplicaId=No node id
			bytes.GetIntFromBigEndianBytes(24).ShouldBe(456);	//MaxWaitTime
			bytes.GetIntFromBigEndianBytes(28).ShouldBe(123);	//MinBytes
			bytes.GetIntFromBigEndianBytes(32).ShouldBe(1);	  //Array size for Topics
			bytes.GetShortFromBigEndianBytes(36).ShouldBe<short>(4);	//Topic string length "test"
			bytes.ShouldBeString(38, "test");                   //Topic
			bytes.GetIntFromBigEndianBytes(42).ShouldBe(1);	    //Array size for Partitions
			bytes.GetIntFromBigEndianBytes(46).ShouldBe(42);	  //Partition
			bytes.GetLongFromBigEndianBytes(50).ShouldBe(4711); //Offset
			bytes.GetIntFromBigEndianBytes(58).ShouldBe(100);	  //MaxBytes, FetchSize
		}
	}
}