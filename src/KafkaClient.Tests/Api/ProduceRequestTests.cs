using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client;
using Kafka.Client.Api;
using KafkaClient.Tests.TestHelpers;
using Xunit;
using Xunit.Extensions;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public class ProduceRequestTests
	{
		[Theory]
		[InlineData(-1, 10)]
		[InlineData(0, 20)]
		[InlineData(1, 3000)]
		[InlineData(42, 566941)]
		public void Given_request_Then_it_serializes_correctly(int requiredAckInt, int ackTimeoutMs)
		{
			var requiredAck = (short)requiredAckInt;
			var request = new ProduceRequest(new[] { new KeyValuePair<TopicAndPartition, IEnumerable<IMessage>>(new TopicAndPartition("test", 0), new[] { new Message("key", "value") }) }, requiredAck, ackTimeoutMs: ackTimeoutMs);
			var bytes = request.Serialize("client", 4711);

			var expectedSize = 2 + 2 + 4 + (2 + 6) +                      //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   2 + 4 +                                    //RequiredAcks + Timeout
			                   4 +                                        //ArraySize_Topics
			                   (2 + 4) + 4 +                            //String_topic + ArraySize_Partitions 
			                   4 + 4 +                               //Partition + MessageSetSize 
			                   8 + 4 +                             //MessageSetItem: Offset + MessageSize
			                   4 + 1 + 1 + (4 + 3) + (4 + 5);    //Crc + MagicByte + Attributes + Key + Value


			bytes.ShouldHaveLength(expectedSize);
			var index = 0;
			bytes.ShouldMatchRequestMessageHeader(ref index, 4711, "client", RequestApiKeys.Produce);

			bytes.GetShort(ref index).ShouldBe(requiredAck);	//RequiredAck
			bytes.GetInt(ref index).ShouldBe(ackTimeoutMs);	  //Timeout
			bytes.GetInt(ref index).ShouldBe(1);	            //ArraySize_Topics

			bytes.ShouldBeShortString(ref index, "test");    //Topic
			bytes.GetInt(ref index).ShouldBe(1);	            //ArraySize_Partitions
			ExpectPartition(bytes, ref index, 0, Tuple.Create("key", "value"));
		}

		[Fact]
		public void Given_more_complex_request_Then_it_serializes_correctly()
		{
			const short requiredAck = 42;
			const int ackTimeoutMs = 12345;
			var request = new ProduceRequest(new[]
			{
				new KeyValuePair<TopicAndPartition, IEnumerable<IMessage>>(new TopicAndPartition("test1", 0), new[] { new Message("key1a", "value1a"),new Message("key1b", "value1b") }),
				new KeyValuePair<TopicAndPartition, IEnumerable<IMessage>>(new TopicAndPartition("test1", 1), new[] { new Message("key1c", "value1c") }),
				new KeyValuePair<TopicAndPartition, IEnumerable<IMessage>>(new TopicAndPartition("test2", 1), new[] { new Message("key2a", "value2a"),new Message("value2b") })
			}, requiredAck, ackTimeoutMs: ackTimeoutMs);
			var bytes = request.Serialize("client", 4711);

			var expectedSize = 2 + 2 + 4 + (2 + 6) +                      //ApiKey + ApiVersion + CorrelationId + String_ClientId
			                   2 + 4 +                                    //RequiredAcks + Timeout
			                   4 +                                        //ArraySize_Topics
			                   (2 + 5) + 4 +                            //String_topic1 + ArraySize_Partitions 
			                   4 + 4 +                               //Partition + MessageSetSize 
			                   8 + 4 +                             //MessageSetItem 1a: Offset + MessageSize
			                   4 + 1 + 1 + (4 + 5) + (4 + 7) +   //   Crc + MagicByte + Attributes + Key + Value
			                   8 + 4 +                             //MessageSetItem 1b: Offset + MessageSize
			                   4 + 1 + 1 + (4 + 5) + (4 + 7) +   //   Crc + MagicByte + Attributes + Key + Value
			                   4 + 4 +                               //Partition + MessageSetSize 
			                   8 + 4 +                             //MessageSetItem 1c: Offset + MessageSize
			                   4 + 1 + 1 + (4 + 5) + (4 + 7) +   //   Crc + MagicByte + Attributes + Key + Value
			                   (2 + 5) + 4 +                            //String_topic2 + ArraySize_Partitions 
			                   4 + 4 +                               //Partition + MessageSetSize 
			                   8 + 4 +                             //MessageSetItem 1a: Offset + MessageSize
			                   4 + 1 + 1 + (4 + 5) + (4 + 7) +   //   Crc + MagicByte + Attributes + Key + Value
			                   8 + 4 +                             //MessageSetItem 1b: Offset + MessageSize
			                   4 + 1 + 1 + (4 + 0) + (4 + 7);    //   Crc + MagicByte + Attributes + Key + Value


			bytes.ShouldHaveLength(expectedSize);
			var index = 0;
			bytes.ShouldMatchRequestMessageHeader(ref index, 4711, "client", RequestApiKeys.Produce);

			bytes.GetShort(ref index).ShouldBe(requiredAck);	//RequiredAck
			bytes.GetInt(ref index).ShouldBe(ackTimeoutMs);	  //Timeout
			bytes.GetInt(ref index).ShouldBe(2);	            //ArraySize_Topics

			bytes.ShouldBeShortString(ref index, "test1");    //Topic
			bytes.GetInt(ref index).ShouldBe(2);	            //ArraySize_Partitions
			ExpectPartition(bytes, ref index, 0, Tuple.Create("key1a", "value1a"), Tuple.Create("key1b", "value1b"));
			ExpectPartition(bytes, ref index, 1, Tuple.Create("key1c", "value1c"));

			bytes.ShouldBeShortString(ref index, "test2");    //Topic
			bytes.GetInt(ref index).ShouldBe(1);	            //ArraySize_Partitions
			ExpectPartition(bytes, ref index, 1, Tuple.Create("key2a", "value2a"), Tuple.Create((string)null, "value2b"));
		}

		private void ExpectPartition(byte[] bytes, ref int index, int partition, params Tuple<string, string>[] keyAndValues)
		{
			var messageSize = keyAndValues.Sum(t => MessagePartsValidator.CalculateMessageSize(t.Item1, t.Item2));
			var messageSetSize = keyAndValues.Length*(8 + 4) + messageSize; //Offset + MessageSizeField+ message size
			bytes.GetInt(ref index).ShouldBe(partition); //Partition
			bytes.GetInt(ref index).ShouldBe(messageSetSize); //MessageSetSize
			foreach(var keyAndValue in keyAndValues)
			{
				bytes.ShouldBeMessageSetItem(ref index, keyAndValue.Item1, keyAndValue.Item2);
			}
		}
	}
}