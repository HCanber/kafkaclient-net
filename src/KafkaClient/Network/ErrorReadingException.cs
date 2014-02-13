using System;
using System.Runtime.Serialization;

namespace Kafka.Client.Network
{
	[Serializable]
	public class ErrorReadingException : Exception
	{
		private readonly int _bytesRead;

		public ErrorReadingException(int bytesRead, string message)
			: base(message)
		{
			_bytesRead = bytesRead;
		}

		public ErrorReadingException(int bytesRead, string message, Exception inner)
			: base(message, inner)
		{
		}

		protected ErrorReadingException(SerializationInfo info,StreamingContext context): base(info, context){}

		public int BytesRead { get { return _bytesRead; } }
	}
}