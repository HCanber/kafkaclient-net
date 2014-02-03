using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	public class FailedMessagesException : KafkaException
	{
		private readonly IReadOnlyCollection<Tuple<object, TopicAndPartition>> _failedMessages;

		protected FailedMessagesException()
		{
		}

		public FailedMessagesException(IReadOnlyCollection<Tuple<object,TopicAndPartition>> failedMessages, string message) : base(message)
		{
			_failedMessages = failedMessages;
		}

		public FailedMessagesException(IReadOnlyCollection<Tuple<object, TopicAndPartition>> failedMessages, string message, Exception inner)
			: base(message, inner)
		{
		}

		protected FailedMessagesException(SerializationInfo info, StreamingContext context) : base(info, context)
		{
		}

		public IReadOnlyCollection<Tuple<object, TopicAndPartition>> FailedMessages
		{
			get { return _failedMessages; }
		}
	}
}