using KafkaClient.Api;
using KafkaClient.Tests.TestHelpers;
using KafkaClient.Utils;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class TopicMetadataRequestTests
	{
		[Fact]
		public void Test_no_topics_writes_correctly()
		{
			var request = TopicMetadataRequest.CreateAllTopics(clientId: "client", correlationId: 4711);
			var bytes = request.Write();

			var expectedSize = 2 + 2 + 4 + (2 + 6) + //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   4;                    //Topics size
			var expectedTotalMessageLength = expectedSize + 4;
			bytes.ShouldHaveLength(expectedTotalMessageLength);
			bytes.GetIntFromBigEndianBytes(0).ShouldBe(expectedSize);	//Size
			bytes.GetShortFromBigEndianBytes(4).ShouldBe<short>(3);	//ApiKey=MetadataRequest
			bytes.GetShortFromBigEndianBytes(6).ShouldBe<short>(0);	//ApiVersion
			bytes.GetIntFromBigEndianBytes(8).ShouldBe(4711);	//CorrelationId
			bytes.GetShortFromBigEndianBytes(12).ShouldBe((short)"client".Length);	//ClientId string length
			bytes.ShouldBeString(14, "client"); 	//ClientId string
			bytes.GetIntFromBigEndianBytes(20).ShouldBe(0);	//Number of topics
		}

		[Fact]
		public void Test_one_topic_writes_correctly()
		{
			var request = TopicMetadataRequest.CreateOneTopic("topic", clientId: "client", correlationId: 4711);
			var bytes = request.Write();

			var expectedSize = 2 + 2 + 4 + (2 + 6) + //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   4 + (2 + 5);          //Topics size + String_Topic
			var expectedTotalMessageLength = expectedSize + 4;
			bytes.ShouldHaveLength(expectedTotalMessageLength);
			bytes.GetIntFromBigEndianBytes(0).ShouldBe(expectedSize);	//Size
			bytes.GetShortFromBigEndianBytes(4).ShouldBe<short>(3);	//ApiKey=MetadataRequest
			bytes.GetShortFromBigEndianBytes(6).ShouldBe<short>(0);	//ApiVersion
			bytes.GetIntFromBigEndianBytes(8).ShouldBe(4711);	//CorrelationId
			bytes.GetShortFromBigEndianBytes(12).ShouldBe<short>((short)"client".Length);	//ClientId string length
			bytes.ShouldBeString(14, "client");	//ClientId string
			bytes.GetIntFromBigEndianBytes(20).ShouldBe(1);	//Number of topics
			bytes.GetShortFromBigEndianBytes(24).ShouldBe<short>((short)"topic".Length);	//Topic string length
			bytes.ShouldBeString(26, "topic");	//topic string
		}


		[Fact]
		public void Test_many_topics_writes_correctly()
		{
			var request = new TopicMetadataRequest(new[] { "topic1", "topic 2" }, clientId: "client", correlationId: 4711);
			var bytes = request.Write();

			var expectedSize = 2 + 2 + 4 + (2 + 6) +           //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   4 + (2 + 6) + (2 + 7);          //Topics size + String_Topic
			var expectedTotalMessageLength = expectedSize + 4;
			bytes.ShouldHaveLength(expectedTotalMessageLength);
			bytes.GetIntFromBigEndianBytes(0).ShouldBe(expectedSize);	//Size
			bytes.GetShortFromBigEndianBytes(4).ShouldBe<short>(3);	//ApiKey=MetadataRequest
			bytes.GetShortFromBigEndianBytes(6).ShouldBe<short>(0);	//ApiVersion
			bytes.GetIntFromBigEndianBytes(8).ShouldBe(4711);	//CorrelationId
			bytes.GetShortFromBigEndianBytes(12).ShouldBe<short>((short)"client".Length);	//ClientId string length
			bytes.ShouldBeString(14, "client");	//ClientId string
			bytes.GetIntFromBigEndianBytes(20).ShouldBe(2);	//Number of topics
			bytes.GetShortFromBigEndianBytes(24).ShouldBe<short>((short)"topic1".Length);	//Topic string length
			bytes.ShouldBeString(26, "topic1");	//topic string
			bytes.GetShortFromBigEndianBytes(32).ShouldBe<short>((short)"topic 2".Length);	//Topic string length
			bytes.ShouldBeString(34, "topic 2");	//topic string
		}
	}
}