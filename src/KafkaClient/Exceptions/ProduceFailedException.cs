using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class ProduceFailedException : KafkaException
	{
		private readonly IReadOnlyCollection<TopicAndPartitionValue<KafkaError>> _errors;

		public ProduceFailedException(string message):base(message)
		{
			_errors = new TopicAndPartitionValue<KafkaError>[0];
		}

		public ProduceFailedException(params TopicAndPartitionValue<KafkaError>[] errors)
		{
			_errors = errors;
		}

		public ProduceFailedException(IReadOnlyCollection<TopicAndPartitionValue<KafkaError>> errors)
		{
			_errors = errors;
		}

		public ProduceFailedException(IReadOnlyCollection<TopicAndPartitionValue<KafkaError>> errors, string message)
			: base(message)
		{
			_errors = errors;
		}

		public ProduceFailedException(IReadOnlyCollection<TopicAndPartitionValue<KafkaError>> errors, string message, Exception inner)
			: base(message, inner)
		{
			_errors = errors;
		}

		protected ProduceFailedException(SerializationInfo info, StreamingContext context) : base(info, context) { }

		public IReadOnlyCollection<TopicAndPartitionValue<KafkaError>> Errors
		{
			get { return _errors; }
		}
	}
}