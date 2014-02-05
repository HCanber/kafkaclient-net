using System.Collections.Generic;
using System.Linq;
using Kafka.Client.IO;
using Kafka.Client.Utils;

namespace Kafka.Client.Api
{
	public class FetchRequest : RequestBase
	{
		private readonly int _minBytes;
		private readonly int _maxWait;
		private readonly List<IGrouping<string, KeyValuePair<TopicAndPartition, PartitionFetchInfo>>> _requestInfoGroupedByTopic;
		public const int DefaultMaxWait = 0;
		public const int DefaultMinBytes = 0;

		public FetchRequest(IEnumerable<KeyValuePair<TopicAndPartition, PartitionFetchInfo>> requestInfo, int minBytes = DefaultMinBytes, int maxWait = DefaultMaxWait)
			: base((short)RequestApiKeys.Fetch)
		{
			_minBytes = minBytes;
			_maxWait = maxWait;
			_requestInfoGroupedByTopic = requestInfo.GroupBy(i => i.Key.Topic).ToList();
		}

		protected virtual int ReplicaId { get { return Request.OrdinaryConsumerId; } }


		protected override void WriteRequestMessage(KafkaWriter writer)
		{
			writer.WriteInt(ReplicaId);
			writer.WriteInt(_maxWait);
			writer.WriteInt(_minBytes);
			writer.WriteRepeated(_requestInfoGroupedByTopic, grp =>
			{
				var topic = grp.Key;
				var partitionFetchInfos = grp.ToList();
				writer.WriteShortString(topic);
				writer.WriteRepeated(partitionFetchInfos, info =>
				{
					var topicAndPartition = info.Key;
					var partitionFetchInfo = info.Value;

					writer.WriteInt(topicAndPartition.Partition);
					writer.WriteLong(partitionFetchInfo.Offset);
					writer.WriteInt(partitionFetchInfo.FetchSize);
				});
			});
		}

		protected override int MessageSizeInBytes
		{
			get
			{
				const int intSize = BitConversion.IntSize;
				const int longSize = BitConversion.LongSize;
				return
					intSize + // ReplicaId
					intSize + // MaxWait
					intSize + // MinBytes
					KafkaWriter.GetArraySize(_requestInfoGroupedByTopic, grp =>
					{
						var topic = grp.Key;
						var partitionFetchInfos = grp.ToList();
						return KafkaWriter.GetShortStringLength(topic) +
									 KafkaWriter.GetArraySize(partitionFetchInfos, info =>
										 intSize + //PartitionId
										 longSize + //Offset
										 intSize //Fetch size
										 );
					});

			}
		}

		public static FetchRequest CreateSingleRequest(string topic, int partition, long offset, int fetchSize, int minBytes = DefaultMinBytes, int maxWait = DefaultMaxWait)
		{
			return new FetchRequest(new[]{new KeyValuePair<TopicAndPartition, PartitionFetchInfo>(
					new TopicAndPartition(topic,partition),
					new PartitionFetchInfo(offset,fetchSize) ) },
				minBytes,
				maxWait
				);
		}
	}
}