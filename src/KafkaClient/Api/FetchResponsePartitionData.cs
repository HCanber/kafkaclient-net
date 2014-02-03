using System.Collections.Generic;
using System.Diagnostics;
using Common.Logging;
using Kafka.Client.IO;

namespace Kafka.Client.Api
{
	public class FetchResponsePartitionData
	{
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();
		private readonly short _error;
		private readonly long _highwaterMarkOffset;
		private readonly IReadOnlyList<MessageSetItem> _messages;

		private FetchResponsePartitionData(short error, long highwaterMarkOffset, IReadOnlyList<MessageSetItem> messages)
		{
			_error = error;
			_highwaterMarkOffset = highwaterMarkOffset;
			_messages = messages;
		}

		public short ErrorValue
		{
			get { return _error; }
		}

		public KafkaError Error
		{
			get { return (KafkaError)_error; }
		}

		public bool HasError
		{
			get { return _error != (short)KafkaError.NoError; }
		}

		public long HighwaterMarkOffset
		{
			get { return _highwaterMarkOffset; }
		}

		public IReadOnlyList<MessageSetItem> Messages
		{
			get { return _messages; }
		}

		public static FetchResponsePartitionData Deserialize(IReadBuffer readBuffer)
		{
			var error = readBuffer.ReadShort();
			var highwaterMarkOffset = readBuffer.ReadLong();
			var messageSetSize = readBuffer.ReadInt();
			var messageSetBuffer = readBuffer.Slice(messageSetSize);
			var messageSet = new List<MessageSetItem>();
			while(messageSetBuffer.BytesLeft > 0)
			{
				var messageSetItem = MessageSetItem.Deserialize(messageSetBuffer);
				messageSet.Add(messageSetItem);
			}
			Debug.Assert(messageSetBuffer.BytesLeft == 0, "messageSetBuffer.BytesLeft should be 0");
			return new FetchResponsePartitionData(error, highwaterMarkOffset, messageSet);
		}

	}
}