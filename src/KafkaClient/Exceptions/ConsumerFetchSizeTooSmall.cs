using System;
using System.Runtime.Serialization;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class ConsumerFetchSizeTooSmall : KafkaException
	{

		protected ConsumerFetchSizeTooSmall(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
		}
			public ConsumerFetchSizeTooSmall(string message)
			: base(message)
		{
		}

		public ConsumerFetchSizeTooSmall(string message, Exception inner)
			: base(message, inner)
		{
		}


	}
}