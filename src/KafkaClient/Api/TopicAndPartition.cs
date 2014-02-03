using System;
using System.Collections;
using System.Globalization;
using System.Text;

namespace Kafka.Client.Api
{
	public class TopicAndPartition : IEquatable<TopicAndPartition>, IComparable<TopicAndPartition>, IStructuralComparable, IStructuralEquatable
	{
		private readonly string _topic;
		private readonly int _partition;

		public TopicAndPartition(string topic, int partition)
		{
			if(topic == null) throw new ArgumentNullException("topic");
			if(topic.Length == 0) throw new ArgumentException("topic must be specified", "topic");
			_topic = topic;
			_partition = partition;
		}

		public string Topic { get { return _topic; } }
		public int Partition { get { return _partition; } }

		public override bool Equals(object obj)
		{
			if(ReferenceEquals(null, obj)) return false;
			if(ReferenceEquals(this, obj)) return true;
			if(obj.GetType() != GetType()) return false;
			return Equals((TopicAndPartition)obj);
		}

		public bool Equals(TopicAndPartition other)
		{
			if(ReferenceEquals(null, other)) return false;
			if(ReferenceEquals(this, other)) return true;
			return string.Equals(_topic, other._topic, StringComparison.OrdinalIgnoreCase) && _partition == other._partition;
		}

		bool IStructuralEquatable.Equals(object other, IEqualityComparer comparer)
		{
			if(ReferenceEquals(null, other)) return false;
			if(ReferenceEquals(this, other)) return true;
			var otherTopicAndPartition = other as TopicAndPartition;
			return otherTopicAndPartition != null
			       && comparer.Equals(_topic, otherTopicAndPartition._topic)
			       && comparer.Equals(_partition, otherTopicAndPartition._partition);
		}


		public int CompareTo(TopicAndPartition other)
		{
			if(other == null) return 1;
			var topicComparison = string.Compare(_topic, other._topic, StringComparison.OrdinalIgnoreCase);
			if(topicComparison != 0) return topicComparison;
			return _partition.CompareTo(other._partition);
		}

		int IStructuralComparable.CompareTo(object other, IComparer comparer)
		{
			if(other == null) return 1;
			var otherTopicAndPartition = other as TopicAndPartition;
			if(otherTopicAndPartition == null)
			{
				throw new ArgumentException(string.Format("Cannot compare a {0} to a {1}.", GetType(), other.GetType()), "other");
			}
			var topicComparison = comparer.Compare(_topic, otherTopicAndPartition._topic);
			if(topicComparison != 0) return topicComparison;
			return comparer.Compare(_partition, otherTopicAndPartition._partition);
		}

		int IStructuralEquatable.GetHashCode(IEqualityComparer comparer)
		{

			return (comparer.GetHashCode(_topic) * 397) ^ comparer.GetHashCode(_partition);

		}

		public override int GetHashCode()
		{
			unchecked
			{
				return (_topic.GetHashCode() * 397) ^ _partition;
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

		public override string ToString()
		{
			var partition = _partition.ToString(CultureInfo.InvariantCulture);
			var sb = new StringBuilder(_topic.Length + 3 + partition.Length);
			sb.Append(_topic);
			sb.Append(" [");
			sb.Append(_partition);
			sb.Append(']');
			return sb.ToString();
		}
	}
}