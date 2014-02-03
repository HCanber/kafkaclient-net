using System;
using System.Runtime.Serialization;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class KafkaException : Exception
	{
		protected KafkaException()
		{
		}

		public KafkaException(string message)
			: base(message)
		{
		}

		public KafkaException(string message, Exception inner)
			: base(message, inner)
		{
		}

		protected KafkaException(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context)
		{
		}
	}
}