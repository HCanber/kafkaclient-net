using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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


		protected Task<ResponseResult<FetchResponse, long>> FetchMessagesAsync(string topic, IEnumerable<KeyValuePair<int, long>> partitionAndOffsets, int fetchMaxBytes, CancellationToken cancellationToken, int minBytes = 1, int maxWaitForMessagesInMs = 100)
		{
			var topicAndPartitionValues = partitionAndOffsets.Select(p => new TopicAndPartitionValue<long>(new TopicAndPartition(topic, p.Key), p.Value)).ToList();
			return FetchMessagesAsync(topicAndPartitionValues, fetchMaxBytes, cancellationToken, minBytes: minBytes, maxWaitForMessagesInMs: maxWaitForMessagesInMs);
		}

		protected Task<ResponseResult<FetchResponse, long>> FetchMessagesAsync(TopicAndPartition topic, long offset, int fetchMaxBytes, CancellationToken cancellationToken, int minBytes = 1, int maxWaitForMessagesInMs = 100)
		{
			return FetchMessagesAsync(new[] { new TopicAndPartitionValue<long>(topic, offset) }, fetchMaxBytes, cancellationToken, minBytes: minBytes, maxWaitForMessagesInMs: maxWaitForMessagesInMs);
		}

		protected Task<ResponseResult<FetchResponse, long>> FetchMessagesAsync(IReadOnlyCollection<TopicAndPartitionValue<long>> topicsAndOffsets, int fetchMaxBytes, CancellationToken cancellationToken, int minBytes = FetchRequest.DefaultMinBytes, int maxWaitForMessagesInMs = FetchRequest.DefaultMaxWait)
		{
			if(fetchMaxBytes<MessageSetItem.SmallestPossibleSize) 
				throw new ArgumentException("maxBytes must be at least "+MessageSetItem.SmallestPossibleSize,"fetchMaxBytes");

			var responses = _client.SendToLeaderAsync(
				topicsAndOffsets, 
				(items, reqId) =>
				{
					var requestInfo = items.Select(t => new KeyValuePair<TopicAndPartition, PartitionFetchInfo>(t.TopicAndPartition, new PartitionFetchInfo(t.Value, fetchMaxBytes)));
					return new FetchRequest(requestInfo,minBytes, maxWaitForMessagesInMs);
				}, 
				FetchResponse.Deserialize, 
				cancellationToken);
			return responses;
		}


	}
}