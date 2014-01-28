using System;
using System.Runtime.Serialization;

namespace KafkaClient.Network
{
	[Serializable]
	public class ErrorReadingException : Exception
	{

		public ErrorReadingException()
		{
		}

		public ErrorReadingException(string message) : base(message)
		{
		}

		public ErrorReadingException(string message, Exception inner) : base(message, inner)
		{
		}

		protected ErrorReadingException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}