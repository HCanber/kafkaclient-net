using System;
using System.Collections.Generic;
using KafkaClient.IO;

namespace KafkaClient.Api
{
	public class TopicMetadataRequest : RequestBase
	{
		private readonly IReadOnlyCollection<string> _topics;
		protected const int DefaultCorrelationId = 0;

		public TopicMetadataRequest(IReadOnlyCollection<string> topics, string clientId, int correlationId = DefaultCorrelationId)
			: base((short)RequestApiKeys.Metadata, correlationId, clientId)
		{
			_topics = topics;
		}

		protected override int MessageSizeInBytes
		{
			get { return KafkaWriter.GetArraySize(_topics, KafkaWriter.GetShortStringLength); }
		}

		protected override void WriteRequestMessage(KafkaWriter writer)
		{
			writer.WriteArray(_topics, writer.WriteShortString);
		}

		public static TopicMetadataRequest CreateAllTopics(string clientId, int correlationId = DefaultCorrelationId)
		{
			return new TopicMetadataRequest(new string[0], clientId, correlationId);
		}


		public static TopicMetadataRequest CreateOneTopic(string topic, string clientId, int correlationId = DefaultCorrelationId)
		{
			return new TopicMetadataRequest(new string[] { topic }, clientId, correlationId);
		}
	}
}