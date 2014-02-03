using System.Collections.Generic;
using System.Linq;
using Common.Logging;
using Kafka.Client.IO;

namespace Kafka.Client.Api
{
	public class TopicData
	{
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();
		private readonly string _topic;
		private readonly IReadOnlyDictionary<int, FetchResponsePartitionData> _partitions;

		private TopicData(string topic, IReadOnlyDictionary<int, FetchResponsePartitionData> partitions)
		{
			_topic = topic;
			_partitions = partitions;
		}

		public string Topic { get { return _topic; } }

		public IReadOnlyDictionary<int, FetchResponsePartitionData> Partitions { get { return _partitions; } }

		public static TopicData Deserialize(IReadBuffer readBuffer)
		{
			var topic = readBuffer.ReadShortString();
			var partitions = readBuffer.ReadArray(p =>
			{
				var partitionId = p.ReadInt();
				var partitionData = FetchResponsePartitionData.Deserialize(p);
				return new { partitionId, partitionData };
			}).ToDictionary(i => i.partitionId, i => i.partitionData);
			return new TopicData(topic, partitions);
		}
	}
}