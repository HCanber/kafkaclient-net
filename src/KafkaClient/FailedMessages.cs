using System.Collections.Generic;

namespace Kafka.Client
{
	public static class FailedMessages
	{
		public static FailedMessages<T> Create<T>(TopicAndPartitionValue<IEnumerable<IMessage>> messages, T error)
		{
			return new FailedMessages<T>(messages, error);
		}
	}

	public class FailedMessages<T>
	{
		private readonly TopicAndPartitionValue<IEnumerable<IMessage>> _messages;
		private readonly T _error;

		public FailedMessages(TopicAndPartitionValue<IEnumerable<IMessage>> messages, T error)
		{
			_messages = messages;
			_error = error;
		}

		public TopicAndPartitionValue<IEnumerable<IMessage>> Messages
		{
			get { return _messages; }
		}

		public T Error
		{
			get { return _error; }
		}
	}
}