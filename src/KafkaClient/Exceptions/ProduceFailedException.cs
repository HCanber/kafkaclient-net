using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using Kafka.Client.Api;
using Kafka.Client.Utils;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class ProduceFailedException : KafkaException
	{
		private readonly IReadOnlyCollection<FailedMessages<Exception>> _failedMessages;
		private readonly IReadOnlyCollection<FailedMessages<KafkaError>> _messagesWithErrors;
		private readonly IReadOnlyCollection<TopicAndPartitionValue<long>> _offsets;

		protected ProduceFailedException(SerializationInfo info, StreamingContext context) : base(info, context) { }

		public ProduceFailedException(string message)
			: base(message)
		{
			_failedMessages = EmptyReadOnly<FailedMessages<Exception>>.Instance;
			_messagesWithErrors = EmptyReadOnly<FailedMessages<KafkaError>>.Instance;
		}

		public ProduceFailedException(IReadOnlyCollection<FailedMessages<Exception>> failedMessages, IReadOnlyCollection<FailedMessages<KafkaError>> messagesWithErrors, IReadOnlyCollection<TopicAndPartitionValue<long>> offsets)
			: base("Errors occurred. FailedMessages and MessagesWithErrors contains details.")
		{
			_failedMessages = failedMessages;
			_messagesWithErrors = messagesWithErrors;
			_offsets = offsets;
		}


		/// <summary>Gets the messages that failed with an exception.</summary>
		public IReadOnlyCollection<FailedMessages<Exception>> FailedMessages
		{
			get { return _failedMessages; }
		}

		/// <summary>Gets the messages that a broker responded with an error.</summary>
		public IReadOnlyCollection<FailedMessages<KafkaError>> MessagesWithErrors
		{
			get { return _messagesWithErrors; }
		}

		/// <summary>Gets the offsets for the topic-partitions that succeeded.</summary>
		public IReadOnlyCollection<TopicAndPartitionValue<long>> Offsets
		{
			get { return _offsets; }
		}

		public override string ToString()
		{
			var sb = new StringBuilder();
			sb.Append(Message);
			AppendMessages(sb, _failedMessages, "Failed messages: ");
			sb.AppendLine();
			AppendMessages(sb, _messagesWithErrors, "Messages with error: ");
			return sb.ToString();
		}

		private static void AppendMessages<T>(StringBuilder sb, IReadOnlyCollection<FailedMessages<T>> messages, string header)
		{
			if(messages.Count > 0)
			{
				sb.AppendLine(header);
				foreach(var message in messages)
				{
					sb.Append(string.Join(", ", message.Messages.TopicAndPartition));
					sb.AppendLine(":");
					sb.AppendLine(message.Error.ToString());
				}
			}
		}
	}
}