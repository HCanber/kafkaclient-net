using System;
using Kafka.Client.IO;

namespace Kafka.Client.Api
{
	public class TooSmallBufferSizeMessage : IMessage
	{
		private static TooSmallBufferSizeMessage _instance = new TooSmallBufferSizeMessage();

		private TooSmallBufferSizeMessage() {/* Intentionally left blank */}

		bool IMessage.HasKey { get { return false; } }
		int IMessage.KeySize { get { return -1; } }
		ArraySegment<byte>? IMessage.Key { get { return null; } }
		int IMessage.ValueSize { get { return -1; } }
		ArraySegment<byte>? IMessage.Value { get { return null; } }
		byte IMessage.Magic { get { return 0; } }
		byte IMessage.Attributes { get { return 0; } }
		uint IMessage.Checksum { get { return 42; } }
		bool IMessage.IsValid { get { return false; } }

		public static TooSmallBufferSizeMessage Instance { get { return _instance; } }

		uint IMessage.ComputeChecksum() { return 4711; }

		int IKafkaRequestPart.GetSize()
		{
			return 0;
		}

		void IKafkaRequestPart.WriteTo(KafkaWriter writer)
		{
			//Intentionally left blank
		}
	}
}