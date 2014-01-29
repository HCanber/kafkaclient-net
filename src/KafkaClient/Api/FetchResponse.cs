using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using KafkaClient.IO;
using KafkaClient.Message;

namespace KafkaClient.Api
{
	public class FetchResponse : ResponseBase, IEnumerable<KeyValuePair<TopicAndPartition, FetchResponsePartitionData>>
	{
		private readonly IReadOnlyDictionary<TopicAndPartition, FetchResponsePartitionData> _data;

		private FetchResponse(int correlationId, IReadOnlyDictionary<TopicAndPartition, FetchResponsePartitionData> data)
			: base(correlationId)
		{
			_data = data;
		}

		public IReadOnlyDictionary<TopicAndPartition, FetchResponsePartitionData> Data
		{
			get { return _data; }
		}

		public IReadOnlyList<MessageSetItem> MessageSet(string topic, int partition)
		{
			var data = _data[new TopicAndPartition(topic, partition)];
			return data.Messages;
		}

		public bool HasError
		{
			get { return _data.Values.Any(d => d.HasError); }
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public IEnumerator<KeyValuePair<TopicAndPartition, FetchResponsePartitionData>> GetEnumerator()
		{
			return _data.GetEnumerator();
		}

		public static FetchResponse Deserialize(IReadBuffer readBuffer)
		{
			var correlationId = readBuffer.ReadInt();
			var data = readBuffer.ReadArray(p =>
			{
				var topicData = TopicData.Deserialize(readBuffer);
				var topic = topicData.Topic;
				return topicData.Partitions.Select(kvp => Tuple.Create(new TopicAndPartition(topic, kvp.Key), kvp.Value));
			}).SelectMany(l => l).ToDictionary(t => t.Item1, t => t.Item2);
			return new FetchResponse(correlationId, data);
		}
	}
}