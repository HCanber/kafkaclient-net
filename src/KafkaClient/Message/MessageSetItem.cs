using KafkaClient.IO;

namespace KafkaClient.Message
{
	public class MessageSetItem
	{
		private readonly long _offset;
		private readonly Message _message;

		private MessageSetItem(long offset, Message message)
		{
			_offset = offset;
			_message = message;
		}

		public long Offset
		{
			get { return _offset; }
		}

		public Message Message
		{
			get { return _message; }
		}

		public static MessageSetItem Deserialize(IReadBuffer readBuffer)
		{
			var offset = readBuffer.ReadLong();
			var messageSize = readBuffer.ReadInt();
			Message message;
			if(messageSize>0)
			{
				var messageBuffer = readBuffer.Slice(messageSize);
				message = Message.Deserialize(messageBuffer, messageSize);
			}
			else
			{
				message = null;
			}
			return new MessageSetItem(offset, message);
		}
	}
}