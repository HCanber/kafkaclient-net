using System;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class UnknownPartitionException : TopicAndPartitionExceptionBase
	{
		// This constructor is needed for serialization.
		protected UnknownPartitionException(SerializationInfo info, StreamingContext context) : base(info, context) { }

		public UnknownPartitionException(TopicAndPartition topicAndPartition)
			: base(topicAndPartition, "Unknown partition: {0}")
		{
		}
	}
}