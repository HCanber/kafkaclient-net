using System;
using System.Runtime.Serialization;

namespace KafkaClient
{
	[Serializable]
	public class KafkaException : Exception
	{
		public KafkaException()
		{
		}

		public KafkaException(string message) : base(message)
		{
		}

		public KafkaException(string message, Exception inner) : base(message, inner)
		{
		}

		protected KafkaException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}