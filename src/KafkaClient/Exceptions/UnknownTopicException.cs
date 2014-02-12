using System;
using System.Runtime.Serialization;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class UnknownTopicException : KafkaException
	{
		private readonly string _topic;

		public UnknownTopicException(string topic)
		{
			_topic = topic;
		}

		protected UnknownTopicException(SerializationInfo info,StreamingContext context) : base(info, context){}

		public string Topic { get { return _topic; } }

		public override string ToString()
		{
			return "Unknown topic: \"" + _topic + "\"";
		}
	}
}