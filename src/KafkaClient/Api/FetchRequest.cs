using System.Collections.Generic;
using System.Linq;
using KafkaClient.IO;
using KafkaClient.Utils;

namespace KafkaClient.Api
{
	public class FetchRequest : RequestBase
	{
		private readonly int _minBytes;
		private readonly int _maxWait;
		private readonly int _correlationId;
		private List<IGrouping<string, KeyValuePair<TopicAndPartition, PartitionFetchInfo>>> _requestInfoGroupedByTopic;
		protected const short CurrentVersion = 0;
		protected const int DefaultMaxWait = 0;
		protected const int DefaultMinBytes = 0;
		protected const int DefaultCorrelationId = 0;

		public FetchRequest(string clientId, IEnumerable<KeyValuePair<TopicAndPartition, PartitionFetchInfo>> requestInfo, int minBytes = DefaultMinBytes, int maxWait = DefaultMaxWait, int correlationId = DefaultCorrelationId)
			: base((short)RequestApiKeys.Fetch, correlationId, clientId)
		{
			_minBytes = minBytes;
			_maxWait = maxWait;
			_correlationId = correlationId;
			_requestInfoGroupedByTopic = requestInfo.GroupBy(i => i.Key.Topic).ToList();
		}

		protected virtual int ReplicaId { get { return Request.OrdinaryConsumerId; } }


		protected override void WriteRequestMessage(KafkaWriter writer)
		{
			writer.WriteInt(ReplicaId);
			writer.WriteInt(_maxWait);
			writer.WriteInt(_minBytes);
			writer.WriteArray(_requestInfoGroupedByTopic, grp =>
			{
				var topic = grp.Key;
				var partitionFetchInfos = grp.ToList();
				writer.WriteShortString(topic);
				writer.WriteArray(partitionFetchInfos, info =>
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

		public static FetchRequest CreateSingleRequest(string topic, int partition, long offset, int fetchSize, string clientId = "", int minBytes=DefaultMinBytes, int maxWait=DefaultMaxWait, int correlationId=DefaultCorrelationId)
		{
			return new FetchRequest(clientId,
				new []{new KeyValuePair<TopicAndPartition, PartitionFetchInfo>(
					new TopicAndPartition(topic,partition),
					new PartitionFetchInfo(offset,fetchSize) ) },
				minBytes,
				maxWait,
				correlationId
				);
		}
	}
}