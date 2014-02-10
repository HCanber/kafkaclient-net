using Kafka.Client.Api;

namespace Kafka.Client
{
	public class TopicAndPartitionValue<T>
	{
		private readonly TopicAndPartition _topicAndPartition;
		private readonly T _value;

		public TopicAndPartitionValue(TopicAndPartition topicAndPartition, T value)
		{
			_topicAndPartition = topicAndPartition;
			_value = value;
		}

		public TopicAndPartition TopicAndPartition
		{
			get { return _topicAndPartition; }
		}

		public T Value
		{
			get { return _value; }
		}
	}
}