using System.Collections.Generic;
using Kafka.Client.IO;

namespace Kafka.Client.Api
{
	public class TopicMetadata
	{
		private readonly string _topic;
		private readonly IReadOnlyList<PartitionMetadata> _partionMetaDatas;
		private readonly short _errorCode;

		private TopicMetadata(string topic, IReadOnlyList<PartitionMetadata> partionMetaDatas, short errorCode)
		{
			_topic = topic;
			_partionMetaDatas = partionMetaDatas;
			_errorCode = errorCode;
		}

		public string Topic { get { return _topic; } }

		public IReadOnlyList<PartitionMetadata> PartionMetaDatas { get { return _partionMetaDatas; } }

		public short ErrorCode { get { return _errorCode; } }



		public static TopicMetadata Deserialize(IReadBuffer buffer, IReadOnlyDictionary<int, Broker> brokersById)
		{
			var errorCode = buffer.ReadShortInRange(-1, short.MaxValue, "Error code");
			var topic = buffer.ReadShortString();
			var partionMetaDatas = buffer.ReadArray(buf => PartitionMetadata.Deserialize(buf, brokersById));
			return new TopicMetadata(topic, partionMetaDatas, errorCode);
		}

	}
}