using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Kafka.Client.Exceptions
{
	public class KafkaInvalidTopicException : KafkaException
	{
		private readonly IReadOnlyCollection<string> _topics;

		protected KafkaInvalidTopicException(params string[] topics)
		{
			_topics = topics;
		}

		public KafkaInvalidTopicException(IReadOnlyCollection<string> topics, string message)
			: base(message)
		{
			_topics = topics;
		}

		public KafkaInvalidTopicException(IReadOnlyCollection<string> topics, string message, Exception inner)
			: base(message, inner)
		{
			_topics = topics;
		}

		protected KafkaInvalidTopicException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
		}

		public IReadOnlyCollection<string> Topics
		{
			get { return _topics; }
		}
	}
}