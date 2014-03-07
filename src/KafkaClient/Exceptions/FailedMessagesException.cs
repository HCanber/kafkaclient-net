using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	public class FailedMessagesException : KafkaException
	{
		private readonly IEnumerable<TopicAndPartitionValue<IEnumerable<IMessage>>> _failedMessages;

		public FailedMessagesException(IEnumerable<TopicAndPartitionValue<IEnumerable<IMessage>>> failedMessages)
		{
			_failedMessages = failedMessages;
		}

		protected FailedMessagesException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
		}


		public IEnumerable<TopicAndPartitionValue<IEnumerable<IMessage>>> FailedMessages
		{
			get { return _failedMessages; }
		}
	}
}