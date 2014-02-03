using System;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	public class PartitionUnavailableException : KafkaInvalidPartitionException
	{
		protected PartitionUnavailableException(TopicAndPartition partition)
			: base(partition)
		{
		}

		public PartitionUnavailableException(TopicAndPartition partition, string message) : base(partition, message)
		{
		}

		public PartitionUnavailableException(TopicAndPartition partition, string message, Exception inner) : base(partition, message, inner)
		{
		}

		protected PartitionUnavailableException(SerializationInfo info, StreamingContext context) : base(info, context)
		{
		}
	}
}