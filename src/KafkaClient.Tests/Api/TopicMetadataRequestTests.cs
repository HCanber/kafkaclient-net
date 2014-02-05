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
			var index = 0;
			bytes.ShouldMatchRequestMessageHeader(ref index, 4711, "client", RequestApiKeys.Metadata);

			bytes.GetInt(ref index).ShouldBe(0);	//Number of topics
		}

		[Fact]
		public void Test_one_topic_writes_correctly()
		{
			var request = TopicMetadataRequest.CreateForTopics("topic");
			var bytes = request.Serialize("client", 4711);

			var expectedSize = 2 + 2 + 4 + (2 + 6) + //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   4 + (2 + 5);          //Topics size + String_Topic
			
			bytes.ShouldHaveLength(expectedSize);
			var index = 0;
			bytes.ShouldMatchRequestMessageHeader(ref index, 4711, "client", RequestApiKeys.Metadata);

			bytes.GetInt(ref index).ShouldBe(1);	//Number of topics
			bytes.ShouldBeShortString(ref index, "topic");	//topic string
		}


		[Fact]
		public void Test_many_topics_writes_correctly()
		{
			var request = new TopicMetadataRequest(new[] { "topic1", "topic 2" });
			var bytes = request.Serialize("client", 4711);

			var expectedSize = 2 + 2 + 4 + (2 + 6) +           //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   4 + (2 + 6) + (2 + 7);          //Topics size + String_Topic

			bytes.ShouldHaveLength(expectedSize);

			var index = 0;
			bytes.ShouldMatchRequestMessageHeader(ref index, 4711, "client", RequestApiKeys.Metadata);

			bytes.GetInt(ref index).ShouldBe(2);	//Number of topics
			bytes.ShouldBeShortString(ref index, "topic1");	//topic string
			bytes.ShouldBeShortString(ref index, "topic 2");	//topic string
		}
	}
}