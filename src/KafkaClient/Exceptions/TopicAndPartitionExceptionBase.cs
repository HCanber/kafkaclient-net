using System;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public abstract class TopicAndPartitionExceptionBase : KafkaException
	{
		private readonly TopicAndPartition _topicAndPartition;
		private readonly string _messageFormat;

		protected TopicAndPartitionExceptionBase(TopicAndPartition topicAndPartition, string messageFormat)
		{
			_topicAndPartition = topicAndPartition;
			_messageFormat = messageFormat;
		}


		protected TopicAndPartitionExceptionBase(SerializationInfo info, StreamingContext context) : base(info, context) { }

		public TopicAndPartition TopicAndPartition { get { return _topicAndPartition; } }

		public override string ToString()
		{
			return string.Format(_messageFormat, TopicAndPartition);
		}
	}
}