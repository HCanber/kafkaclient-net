using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Api;

namespace Kafka.Client
{
	public abstract class ConsumerBase
	{
		private readonly IKafkaClient _client;

		public ConsumerBase(IKafkaClient client)
		{
			if(client == null) throw new ArgumentNullException("client");
			_client = client;
		}

		protected IKafkaClient Client { get { return _client; } }


		protected IReadOnlyCollection<FetchResponse> FetchMessages(string topic, IEnumerable<KeyValuePair<int, long>> partitionAndOffsets, out IReadOnlyCollection<TopicAndPartitionValue<long>> failedItems, int fetchMaxBytes, int minBytes = 1, int maxWaitForMessagesInMs = 100)
		{
			return FetchMessages(partitionAndOffsets.Select(p => new TopicAndPartitionValue<long>(new TopicAndPartition(topic, p.Key), p.Value)).ToList(), out failedItems, fetchMaxBytes, minBytes, maxWaitForMessagesInMs);
		}

		protected IReadOnlyCollection<FetchResponse> FetchMessages(TopicAndPartition topic, long offset, out IReadOnlyCollection<TopicAndPartitionValue<long>> failedItems, int fetchMaxBytes, int minBytes = 1, int maxWaitForMessagesInMs = 100)
		{
			return FetchMessages(new[] { new TopicAndPartitionValue<long>(topic, offset) }, out failedItems, fetchMaxBytes, minBytes, maxWaitForMessagesInMs);
		}

		protected IReadOnlyCollection<FetchResponse> FetchMessages(IReadOnlyCollection<TopicAndPartitionValue<long>> topicsAndOffsets, out IReadOnlyCollection<TopicAndPartitionValue<long>> failedItems, int fetchMaxBytes, int minBytes = FetchRequest.DefaultMinBytes, int maxWaitForMessagesInMs = FetchRequest.DefaultMaxWait)
		{
			if(fetchMaxBytes<MessageSetItem.SmallestPossibleSize) 
				throw new ArgumentException("maxBytes must be at least "+MessageSetItem.SmallestPossibleSize,"fetchMaxBytes");

			var responses = _client.SendToLeader(topicsAndOffsets, (items, reqId) => new FetchRequest(items.Select(t => new KeyValuePair<TopicAndPartition, PartitionFetchInfo>(t.TopicAndPartition, new PartitionFetchInfo(t.Value, fetchMaxBytes))), minBytes, maxWaitForMessagesInMs), FetchResponse.Deserialize, out failedItems);
			return responses;
		}


	}
}