using Kafka.Client.Api;
using Kafka.Client.Utils;
using KafkaClient.Tests.TestHelpers;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class TopicMetadataRequestTests
	{
		[Fact]
		public void Test_no_topics_writes_correctly()
		{
			var request = TopicMetadataRequest.CreateAllTopics();
			var bytes = request.Serialize("client",4711);

			var expectedSize = 2 + 2 + 4 + (2 + 6) + //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   4;                    //Topics size
			
			bytes.ShouldHaveLength(expectedSize);
			bytes.GetShortFromBigEndianBytes(0).ShouldBe<short>(3);	//ApiKey=MetadataRequest
			bytes.GetShortFromBigEndianBytes(2).ShouldBe<short>(0);	//ApiVersion
			bytes.GetIntFromBigEndianBytes(4).ShouldBe(4711);	//CorrelationId
			bytes.GetShortFromBigEndianBytes(8).ShouldBe((short)"client".Length);	//ClientId string length
			bytes.ShouldBeString(10, "client"); 	//ClientId string
			bytes.GetIntFromBigEndianBytes(16).ShouldBe(0);	//Number of topics
		}

		[Fact]
		public void Test_one_topic_writes_correctly()
		{
			var request = TopicMetadataRequest.CreateForTopics("topic");
			var bytes = request.Serialize("client", 4711);

			var expectedSize = 2 + 2 + 4 + (2 + 6) + //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   4 + (2 + 5);          //Topics size + String_Topic
			
			bytes.ShouldHaveLength(expectedSize);
			bytes.GetShortFromBigEndianBytes(0).ShouldBe<short>(3);	//ApiKey=MetadataRequest
			bytes.GetShortFromBigEndianBytes(2).ShouldBe<short>(0);	//ApiVersion
			bytes.GetIntFromBigEndianBytes(4).ShouldBe(4711);	//CorrelationId
			bytes.GetShortFromBigEndianBytes(8).ShouldBe<short>((short)"client".Length);	//ClientId string length
			bytes.ShouldBeString(10, "client");	//ClientId string
			bytes.GetIntFromBigEndianBytes(16).ShouldBe(1);	//Number of topics
			bytes.GetShortFromBigEndianBytes(20).ShouldBe<short>((short)"topic".Length);	//Topic string length
			bytes.ShouldBeString(22, "topic");	//topic string
		}


		[Fact]
		public void Test_many_topics_writes_correctly()
		{
			var request = new TopicMetadataRequest(new[] { "topic1", "topic 2" });
			var bytes = request.Serialize("client", 4711);

			var expectedSize = 2 + 2 + 4 + (2 + 6) +           //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   4 + (2 + 6) + (2 + 7);          //Topics size + String_Topic

			bytes.ShouldHaveLength(expectedSize);
			bytes.GetShortFromBigEndianBytes(0).ShouldBe<short>(3);	//ApiKey=MetadataRequest
			bytes.GetShortFromBigEndianBytes(2).ShouldBe<short>(0);	//ApiVersion
			bytes.GetIntFromBigEndianBytes(4).ShouldBe(4711);	//CorrelationId
			bytes.GetShortFromBigEndianBytes(8).ShouldBe<short>((short)"client".Length);	//ClientId string length
			bytes.ShouldBeString(10, "client");	//ClientId string
			bytes.GetIntFromBigEndianBytes(16).ShouldBe(2);	//Number of topics
			bytes.GetShortFromBigEndianBytes(20).ShouldBe<short>((short)"topic1".Length);	//Topic string length
			bytes.ShouldBeString(22, "topic1");	//topic string
			bytes.GetShortFromBigEndianBytes(28).ShouldBe<short>((short)"topic 2".Length);	//Topic string length
			bytes.ShouldBeString(30, "topic 2");	//topic string
		}
	}
}