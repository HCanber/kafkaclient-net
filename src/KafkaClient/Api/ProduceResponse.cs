using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kafka.Client.IO;
using Kafka.Client.Utils;

namespace Kafka.Client.Api
{
	public class ProduceResponse : ResponseBase
	{
		private readonly IReadOnlyDictionary<string, IReadOnlyList<ProducerResponseStatus>> _statusByTopic;
		private readonly bool _hasError;

		public ProduceResponse(int correlationId, IEnumerable<ProducerResponseStatus> statuses)
			: base(correlationId)
		{
			var hasError = false;

			_statusByTopic =new ReadOnlyDictionary<string,IReadOnlyList<ProducerResponseStatus>>(statuses.GroupByToReadOnlyListDictionary(status =>
			{
				hasError |= status.Error != KafkaError.NoError;

				return status.TopicAndPartition.Topic;
			}));

			_hasError = hasError;
		}

		public bool HasError{get { return _hasError; }}

		public int NumberOfTopics { get { return _statusByTopic.Count; } }

		public IReadOnlyDictionary<string, IReadOnlyList<ProducerResponseStatus>> StatusesByTopic { get { return _statusByTopic; } }

	
		public static ProduceResponse Deserialize(IReadBuffer readBuffer)
		{
			var correlationId = readBuffer.ReadInt();
			var data = readBuffer.ReadArray(buffer =>
			{
				var topic = buffer.ReadShortString();
				return readBuffer.ReadArray(buffer2 =>
				{
					var partition = buffer2.ReadInt();
					var errorCode = buffer2.ReadShort();
					var offset = buffer2.ReadLong();
					return new ProducerResponseStatus(new TopicAndPartition(topic, partition), errorCode, offset);
				});
			}).SelectMany(r => r).ToList();
			return new ProduceResponse(correlationId, data);
		}

	}

	public class ProducerResponseStatus
	{
		private readonly TopicAndPartition _topicAndPartition;
		private readonly short _errorCode;
		private readonly long _offset;

		public ProducerResponseStatus(TopicAndPartition topicAndPartition, short errorCode, long offset)
		{
			_topicAndPartition = topicAndPartition;
			_errorCode = errorCode;
			_offset = offset;
		}

		public short ErrorCode { get { return _errorCode; } }
		public KafkaError Error { get { return (KafkaError)_errorCode; } }

		public long Offset { get { return _offset; } }

		public TopicAndPartition TopicAndPartition { get { return _topicAndPartition; } }
	}
}
