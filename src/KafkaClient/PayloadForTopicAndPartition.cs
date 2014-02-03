using Kafka.Client.Api;

namespace Kafka.Client
{
	public class PayloadForTopicAndPartition<T>
	{
		private readonly TopicAndPartition _topicAndPartition;
		private readonly T _payload;

		public PayloadForTopicAndPartition(TopicAndPartition topicAndPartition, T payload)
		{
			_topicAndPartition = topicAndPartition;
			_payload = payload;
		}

		public TopicAndPartition TopicAndPartition
		{
			get { return _topicAndPartition; }
		}

		public T Payload
		{
			get { return _payload; }
		}
	}
}