using System;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class CrcInvalid : KafkaException
	{
		private readonly TopicAndPartition _topicAndPartition;
		private readonly long _offset;
		private readonly uint _expected;
		private readonly uint _actual;

		public CrcInvalid(TopicAndPartition topicAndPartition, long offset, uint expected, uint actual, string message=null)
			: base(message)
		{
			_topicAndPartition = topicAndPartition;
			_offset = offset;
			_expected = expected;
			_actual = actual;
		}

		public TopicAndPartition TopicAndPartition
		{
			get { return _topicAndPartition; }
		}

		public long Offset
		{
			get { return _offset; }
		}

		public uint Expected
		{
			get { return _expected; }
		}

		public uint Actual
		{
			get { return _actual; }
		}

		protected CrcInvalid(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context)
		{
		}
	}
}