using System;

namespace KafkaClient.Api
{
	public class TopicAndPartition : IEquatable<TopicAndPartition>
	{
		private readonly string _topic;
		private readonly int _partition;

		public TopicAndPartition(string topic, int partition)
		{
			_topic = topic;
			_partition = partition;
		}

		public string Topic{get { return _topic; }}

		public int Partition{get { return _partition; }}

		public Tuple<string, int> ToTuple()
		{
			return new Tuple<string, int>(_topic, _partition);
		}

		public override string ToString()
		{
			return string.Format("[{0},{1}]", _topic, _partition);
		}

		public bool Equals(TopicAndPartition other)
		{
			if(ReferenceEquals(null, other)) return false;
			if(ReferenceEquals(this, other)) return true;
			return string.Equals(_topic, other._topic) && _partition == other._partition;
		}

		public override bool Equals(object obj)
		{
			if(ReferenceEquals(null, obj)) return false;
			if(ReferenceEquals(this, obj)) return true;
			if(obj.GetType() != this.GetType()) return false;
			return Equals((TopicAndPartition) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return (_topic.GetHashCode()*397) ^ _partition;
			}
		}

		public static bool operator ==(TopicAndPartition left, TopicAndPartition right)
		{
			return Equals(left, right);
		}

		public static bool operator !=(TopicAndPartition left, TopicAndPartition right)
		{
			return !Equals(left, right);
		}
	}
}