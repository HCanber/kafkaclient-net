using System;
using System.Runtime.Serialization;

namespace Kafka.Client.Network
{
	[Serializable]
	public class SendFailedException : Exception
	{
		// Guidelines: http://msdn.microsoft.com/en-us/library/vstudio/ms229064(v=vs.100).aspx

		// This constructor is needed for serialization.
		protected SendFailedException(SerializationInfo info, StreamingContext context ) : base( info, context ) { }

		public SendFailedException() { }

		public SendFailedException(string message ) : base( message ) { }

		public SendFailedException(string message, Exception inner) : base(message, inner) { }

	}
}