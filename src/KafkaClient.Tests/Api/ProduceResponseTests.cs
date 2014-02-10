using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Api;
using Kafka.Client.IO;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class ProduceResponseTests
	{
		[Fact]
		public void Given_a_response_with_single_topic_and_partition_Then_it_deserialize_correctly()
		{
			var bytes = new byte[]
			{
				0x12, 0x34, 0x56, 0x78, //CorrelationId
				0x00, 0x00, 0x00, 0x01, //Number of topics
				0x00, 0x01,                                      //  Topic, length
				0x54,                                            //  Topic "T"
				0x00, 0x00, 0x00, 0x01,                          //  Number of partitions
				0x01, 0x02, 0x03, 0x04,                          //    Partition
				0x00, 0x00,                                      //    Error code
				0x11, 0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x24,  //    Offset
			};

			var readBuffer = new ReadBuffer(bytes);
			var produceResponse = ProduceResponse.Deserialize(readBuffer);

			readBuffer.BytesLeft.ShouldBe(0);
			produceResponse.CorrelationId.ShouldBe(0x12345678);
			produceResponse.HasError.ShouldBeFalse();
			produceResponse.NumberOfTopics.ShouldBe(1);

			IReadOnlyList<ProducerResponseStatus> statuses;
			produceResponse.StatusesByTopic.ShouldHaveCount(1);

			produceResponse.StatusesByTopic.TryGetValue("T", out statuses).ShouldBeTrue();
			statuses.ShouldHaveCount(1);
			var status = statuses.First();
			status.TopicAndPartition.ShouldBe(new TopicAndPartition("T",0x01020304));
			status.Error.ShouldBe(KafkaError.NoError);
			status.ErrorCode.ShouldBe<short>(0);
			status.Offset.ShouldBe(0x1112131421222324);
		}
		[Fact]
		public void Given_a_response_with_multi_topics_and_partitions_Then_it_deserialize_correctly()
		{
			var bytes = new byte[]
			{
				0x12, 0x34, 0x56, 0x78, //CorrelationId
				0x00, 0x00, 0x00, 0x02, //Number of topics
				0x00, 0x01,                                      //  1 Topic, length
				(byte)'T',                                       //  1 Topic Name
				0x00, 0x00, 0x00, 0x02,                          //  1 Number of partitions
				0x01, 0x02, 0x03, 0x04,                          //  1  1 Partition
				0x00, 0x00,                                      //  1  1 Error code
				0x11, 0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x24,  //  1  1 Offset
				0x01, 0x02, 0x03, 0x05,                          //  1  2 Partition
				0x00, 0x00,                                      //  1  2 Error code
				0x11, 0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x25,  //  1  2 Offset
				0x00, 0x01,                                      //  2 Topic, length
				(byte) 'U',                                      //  2 Topic Name
				0x00, 0x00, 0x00, 0x01,                          //  2 Number of partitions
				0x01, 0x02, 0x03, 0x06,                          //  2  1 Partition
				0x00, 0x00,                                      //  2  1 Error code
				0x11, 0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x26,  //  2  1 Offset
			};

			var readBuffer = new ReadBuffer(bytes);
			var produceResponse = ProduceResponse.Deserialize(readBuffer);

			readBuffer.BytesLeft.ShouldBe(0);
			produceResponse.CorrelationId.ShouldBe(0x12345678);
			produceResponse.HasError.ShouldBeFalse();
			produceResponse.NumberOfTopics.ShouldBe(2);

			IReadOnlyList<ProducerResponseStatus> firstStatuses;
			produceResponse.StatusesByTopic.ShouldHaveCount(2);
			produceResponse.StatusesByTopic.TryGetValue("T", out firstStatuses).ShouldBeTrue();
			firstStatuses.ShouldHaveCount(2);
			var status11 = firstStatuses[0];
			status11.TopicAndPartition.ShouldBe(new TopicAndPartition("T", 0x01020304));
			status11.Error.ShouldBe(KafkaError.NoError);
			status11.ErrorCode.ShouldBe<short>(0);
			status11.Offset.ShouldBe(0x1112131421222324);
			var status12 = firstStatuses[1];
			status12.TopicAndPartition.ShouldBe(new TopicAndPartition("T", 0x01020305));
			status12.Error.ShouldBe(KafkaError.NoError);
			status12.ErrorCode.ShouldBe<short>(0);
			status12.Offset.ShouldBe(0x1112131421222325);

			IReadOnlyList<ProducerResponseStatus> secondStatuses;
			produceResponse.StatusesByTopic.TryGetValue("U", out secondStatuses).ShouldBeTrue();
			secondStatuses.ShouldHaveCount(1);
			var status21 = secondStatuses[0];
			status21.TopicAndPartition.ShouldBe(new TopicAndPartition("U", 0x01020306));
			status21.Error.ShouldBe(KafkaError.NoError);
			status21.ErrorCode.ShouldBe<short>(0);
			status21.Offset.ShouldBe(0x1112131421222326);

		}
	}
}