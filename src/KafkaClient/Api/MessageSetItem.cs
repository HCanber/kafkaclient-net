using Common.Logging;
using Kafka.Client.IO;
using Kafka.Client.Utils;

namespace Kafka.Client.Api
{
	public class MessageSetItem : IMessageSetItem, IKafkaRequestPart
	{
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();

		private readonly long _offset;
		private readonly IMessage _message;

		public MessageSetItem(long offset, IMessage message)
		{
			_offset = offset;
			_message = message;
		}

		public long Offset
		{
			get { return _offset; }
		}

		public IMessage Message
		{
			get { return _message; }
		}

		public static int SmallestPossibleSize
		{
			get
			{
				return BitConversion.LongSize +	//Offset
				       BitConversion.IntSize;   //Message Size-field
			}
		}

		public int GetSize()
		{
			return BitConversion.LongSize + //Offset
			       BitConversion.IntSize +   //Message Size-field
			       _message.GetSize();
		}

		public void WriteTo(KafkaWriter writer)
		{
			writer.WriteLong(_offset);
			writer.WriteInt(_message.GetSize());
			_message.WriteTo(writer);
		}

		public static MessageSetItem Deserialize(IReadBuffer readBuffer)
		{
			const int requiredHeaderSize = BitConversion.LongSize+BitConversion.IntSize;
			var bytesLeft = readBuffer.BytesLeft;
			if(bytesLeft < requiredHeaderSize)
			{
				_Logger.TraceFormat(string.Format("Did not receive entire message. Available={0} bytes. Returning a {1}.", bytesLeft, typeof(TooSmallBufferSizeMessage)));
				readBuffer.Skip(bytesLeft);	//Consume the rest of the buffer
				return new MessageSetItem(-1, TooSmallBufferSizeMessage.Instance);
			}

			var offset = readBuffer.ReadLong();
			var messageSize = readBuffer.ReadInt();
			_Logger.Info(string.Format("Offset: {0}. Size: {1}", offset, messageSize));
			IMessage message;
			if(messageSize>0)
			{
				bytesLeft = readBuffer.BytesLeft;
				if(bytesLeft<messageSize)
				{
					_Logger.TraceFormat(string.Format("Did not receive entire message for offset={2}. Required={0} bytes, available={1} bytes. Returning a {3}.", messageSize, bytesLeft, offset, typeof(TooSmallBufferSizeMessage)));
					message = TooSmallBufferSizeMessage.Instance;
					readBuffer.Skip(bytesLeft);	//Consume the rest of the buffer
				}
				else
				{
					var messageBuffer = readBuffer.Slice(messageSize);
					message = Api.Message.Deserialize(messageBuffer, messageSize);					
				}
			}
			else
			{
				message = null;
			}
			return new MessageSetItem(offset, message);
		}
	}
}