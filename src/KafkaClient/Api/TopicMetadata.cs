using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Kafka.Client.IO;

namespace Kafka.Client.Api
{
	public class TopicMetadata
	{
		private readonly string _topic;
		private readonly ReadOnlyDictionary<int, PartitionMetadata> _partionMetaDatas;
		private readonly short _errorCode;

		public TopicMetadata(string topic, IEnumerable<PartitionMetadata> partionMetaDatas, KafkaError error)
			:this(topic,partionMetaDatas,(short)error)
		{
			//Intentionally left blank
		}

		private TopicMetadata(string topic, IEnumerable<PartitionMetadata> partionMetaDatas, short errorCode)
		{
			_topic = topic;
			_partionMetaDatas = new ReadOnlyDictionary<int, PartitionMetadata>(partionMetaDatas.ToDictionary(p => p.PartitionId));
			_errorCode = errorCode;
		}

		public string Topic { get { return _topic; } }

		public IReadOnlyDictionary<int, PartitionMetadata> PartionMetaDatas { get { return _partionMetaDatas; } }

		public KafkaError Error { get { return (KafkaError) _errorCode; } }
		public short ErrorCode { get { return _errorCode; } }



		public static TopicMetadata Deserialize(IReadBuffer buffer, IReadOnlyDictionary<int, Broker> brokersById)
		{
			var errorCode = buffer.ReadShortInRange(-1, short.MaxValue, "Error code");
			var topic = buffer.ReadShortString();
			var partionMetaDatas =buffer.ReadArray(buf => PartitionMetadata.Deserialize(buf, brokersById));
			return new TopicMetadata(topic, partionMetaDatas, errorCode);
		}

	}
}