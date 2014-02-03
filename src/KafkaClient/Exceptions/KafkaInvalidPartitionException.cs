using System;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	public class KafkaInvalidPartitionException : KafkaException
	{
		private readonly TopicAndPartition _partition;

		protected KafkaInvalidPartitionException(TopicAndPartition partition)
		{
			_partition = partition;
		}

		public KafkaInvalidPartitionException(TopicAndPartition partition, string message)
			: base(message)
		{
			_partition = partition;
		}

		public KafkaInvalidPartitionException(TopicAndPartition partition, string message, Exception inner)
			: base(message, inner)
		{
			_partition = partition;
		}

		protected KafkaInvalidPartitionException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
		}

		public TopicAndPartition Partition
		{
			get { return _partition; }
		}
	}
}