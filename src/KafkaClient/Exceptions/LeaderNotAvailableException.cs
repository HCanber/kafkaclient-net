using System;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class LeaderNotAvailableException : TopicAndPartitionExceptionBase
	{

		public LeaderNotAvailableException(TopicAndPartition topicAndPartition)
			: base(topicAndPartition, "Leader not available for {0}")
		{
		}

		protected LeaderNotAvailableException(TopicAndPartition topicAndPartition, string messageFormat)
			: base(topicAndPartition,messageFormat)
		{
		}



		protected LeaderNotAvailableException(SerializationInfo info, StreamingContext context) : base(info, context) { }

	}
}