using System;
using System.Runtime.Serialization;

namespace KafkaClient.Network
{
	[Serializable]
	public class ClosedChannelException : Exception
	{
		//
		// For guidelines regarding the creation of new exception types, see
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
		// and
		//    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
		//

		public ClosedChannelException():this("This channel is closed")
		{
		}

		public ClosedChannelException(string message) : base(message)
		{
		}

		public ClosedChannelException(string message, Exception inner) : base(message, inner)
		{
		}

		protected ClosedChannelException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
}