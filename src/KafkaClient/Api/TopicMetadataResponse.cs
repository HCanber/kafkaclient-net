using System;
using System.Collections.Generic;
using System.Linq;
using KafkaClient.IO;

namespace KafkaClient.Api
{
	public class TopicMetadataResponse : ResponseBase
	{
		private readonly IReadOnlyDictionary<int, Broker> _brokersById;
		private readonly IReadOnlyList<TopicMetadata> _topicMetadatas;


		private TopicMetadataResponse(int correlationId, IReadOnlyDictionary<int, Broker> brokersById, IReadOnlyList<TopicMetadata> topicMetadatas)
			: base(correlationId)
		{
			_brokersById = brokersById;
			_topicMetadatas = topicMetadatas;
		}

		public IReadOnlyDictionary<int, Broker> BrokersById { get { return _brokersById; } }

		public IReadOnlyList<TopicMetadata> TopicMetadatas { get { return _topicMetadatas; } }

		public static TopicMetadataResponse Deserialize(IReadBuffer readBuffer)
		{
			var correlationId = readBuffer.ReadInt();
			var brokers = readBuffer.ReadArray(b => Broker.Deserialize(readBuffer));
			var brokersById = brokers.ToDictionary(b => b.NodeId);
			var topicMetadatas = readBuffer.ReadArray(b => TopicMetadata.Deserialize(readBuffer, brokersById));
			return new TopicMetadataResponse(correlationId, brokersById, topicMetadatas);
		}
	}
}