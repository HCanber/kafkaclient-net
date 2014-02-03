using System.Collections.Generic;
using Kafka.Client.IO;

namespace Kafka.Client.Api
{
	public class TopicMetadataRequest : RequestBase
	{
		private readonly IReadOnlyCollection<string> _topics;

		public TopicMetadataRequest(IReadOnlyCollection<string> topics)
			: base((short)RequestApiKeys.Metadata)
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

		public static TopicMetadataRequest CreateAllTopics()
		{
			return new TopicMetadataRequest(new string[0]);
		}


		public static TopicMetadataRequest CreateForTopics(params string[] topics)
		{
			return new TopicMetadataRequest(topics);
		}
	}
}