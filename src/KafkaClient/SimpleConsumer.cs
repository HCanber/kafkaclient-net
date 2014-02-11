using System;
using System.Collections.Generic;
using System.Linq;
using Common.Logging;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;
using Kafka.Client.Utils;

namespace Kafka.Client
{
	public class SimpleConsumer : ConsumerBase
	{
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();
		public const int DefaultFetchSize = 4 * 1024;
		public const int DefaultMaxFetchSize = 8 * DefaultFetchSize;
		private readonly string _topic;
		private int _fetchSizeBytes;
		private readonly int? _maxFetchSizeBytes;
		private readonly int _maxWaitTimeInMs;
		private Lazy<Dictionary<int, long>> _offsetsPerPartition;

		public SimpleConsumer(IKafkaClient client, string topic, IEnumerable<int> onlyTheesePartitions = null, int fetchSizeBytes = DefaultFetchSize, int? maxFetchSizeBytes = DefaultMaxFetchSize, int maxWaitTimeInMs = 1000, IEnumerable<KeyValuePair<int, long>> startOffsets = null)
			: base(client)
		{
			if(topic == null) throw new ArgumentNullException("topic");
			if(topic.Length == 0) throw new ArgumentNullException("topic");
			_topic = topic;
			_fetchSizeBytes = fetchSizeBytes;
			_maxFetchSizeBytes = maxFetchSizeBytes;
			_maxWaitTimeInMs = maxWaitTimeInMs;
			var arrOnlyPartitions = onlyTheesePartitions == null ? null : onlyTheesePartitions.OrderBy(i => i).ToArray();
			{
				_offsetsPerPartition = new Lazy<Dictionary<int, long>>(() =>
				{
					var partitions = GetPartitionsForTopic();
					if(arrOnlyPartitions != null)
					{
						partitions = partitions.Where(p => Array.BinarySearch(arrOnlyPartitions, p) >= 0).ToList();
					}
					var dictionary = partitions.ToDictionary(partition => partition, _ => 0L);
					startOffsets.ForEach(kvp => dictionary[kvp.Key] = kvp.Value);
					return dictionary;
				});
			}
		}

		public IEnumerable<IMessageSetItem> GetMessages()
		{
			var offsetsPerPartition = _offsetsPerPartition.Value;
			var partitionsToGet = offsetsPerPartition.Keys.ToList();
			while(partitionsToGet.Count > 0)
			{
				IReadOnlyCollection<TopicAndPartitionValue<long>> failedItems;
				var partitionAndOffsets = partitionsToGet.Select(p => new KeyValuePair<int, long>(p, offsetsPerPartition[p]));
				var responses = FetchMessages(_topic, partitionAndOffsets, out failedItems, fetchMaxBytes: _fetchSizeBytes, maxWaitForMessagesInMs: _maxWaitTimeInMs);
				partitionsToGet.Clear();
				if(failedItems != null && failedItems.Count > 0)
					throw new FetchFailed(failedItems.Select(p => Tuple.Create(p.TopicAndPartition, (FetchResponsePartitionData)null)).ToList(), "Failures occurred when fetching partitions and offsets: " + string.Join(", ", failedItems.Select(t => t.TopicAndPartition + ":" + t.Value)));

				var errorResponses = responses.SelectMany(r => r.Data).Where(kvp => kvp.Value.HasError).Select(kvp => Tuple.Create(kvp.Key, kvp.Value)).ToList();
				if(errorResponses.Count > 0)
					throw new FetchFailed(errorResponses, "Fetch Error: " + string.Join(", ", errorResponses.Select(t => t.Item1 + ":" + t.Item2.Error)));


				foreach(var response in responses)
				{
					foreach(var dataByTopicAndPartition in response.Data)
					{
						var topicAndPartition = dataByTopicAndPartition.Key;
						var partition = topicAndPartition.Partition;
						foreach(var messageSetItem in dataByTopicAndPartition.Value.Messages)
						{
							var message = messageSetItem.Message;
							var tooSmallBufferSizeMessage = message as TooSmallBufferSizeMessage;
							if(tooSmallBufferSizeMessage != null)
							{
								if(_maxFetchSizeBytes.HasValue && _fetchSizeBytes == _maxFetchSizeBytes)
								{
									_Logger.ErrorFormat(string.Format("MaxFetchSizeBytes must be increased above {0} bytes in order to receive {1}, offset {2}.", _maxFetchSizeBytes, topicAndPartition, offsetsPerPartition[partition]));
									throw new ConsumerFetchSizeTooSmall(string.Format("Could not get the whole message from {0}. MaxFetchSizeBytes must be increased.", topicAndPartition));
								}
								//Increase the fetchSize. Cap to MaxFetchSize
								_fetchSizeBytes = _maxFetchSizeBytes.HasValue ? Math.Min(_maxFetchSizeBytes.Value, _fetchSizeBytes * 2) : _fetchSizeBytes * 2;
								_Logger.InfoFormat("Could not receive the message from {1}, offset {2} as fetchSize is to small. Increased the fetchSize to {0}. Will retry.", _fetchSizeBytes, topicAndPartition, offsetsPerPartition[partition]);
								partitionsToGet.Add(partition);
							}
							else if(!message.IsValid)
							{
								throw new CrcInvalid(topicAndPartition, messageSetItem.Offset, message.Checksum, message.ComputeChecksum());
							}
							else
							{
								var currentOffset = offsetsPerPartition[partition];
								var offset = messageSetItem.Offset;
								if(offset >= currentOffset)
								{
									offsetsPerPartition[partition] = offset + 1;
								}
								yield return messageSetItem;
							}
						}
					}
				}
				if(_Logger.IsTraceEnabled && partitionsToGet.Count > 0)
				{
					_Logger.TraceFormat(string.Format("Retrying the following partitions for topic \"{0}\": {1}", _topic, string.Join(",", partitionsToGet)));
				}
			}
		}

		private IReadOnlyCollection<int> GetPartitionsForTopic()
		{
			return Client.GetPartitionsForTopics(new[] { _topic }).First().Value;
		}
	}
}