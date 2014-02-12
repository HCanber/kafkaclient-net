using System;
using System.Collections.Generic;
using Kafka.Client.Api;

namespace Kafka.Client
{
	public class TopicAndPartitionValue<T> : IEquatable<TopicAndPartitionValue<T>>
	{
		private readonly TopicAndPartition _topicAndPartition;
		private readonly T _value;

		public TopicAndPartitionValue(TopicAndPartition topicAndPartition, T value)
		{
			if(topicAndPartition == null) throw new ArgumentNullException("topicAndPartition");
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

		public bool Equals(TopicAndPartitionValue<T> other)
		{
			if(ReferenceEquals(null, other)) return false;
			if(ReferenceEquals(this, other)) return true;
			return Equals(_topicAndPartition, other._topicAndPartition) && EqualityComparer<T>.Default.Equals(_value, other._value);
		}

		public override bool Equals(object obj)
		{
			if(ReferenceEquals(null, obj)) return false;
			if(ReferenceEquals(this, obj)) return true;
			if(obj.GetType() != GetType()) return false;
			return Equals((TopicAndPartitionValue<T>) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return _topicAndPartition.GetHashCode()*397 ^ EqualityComparer<T>.Default.GetHashCode(_value);
			}
		}

		public static bool operator ==(TopicAndPartitionValue<T> left, TopicAndPartitionValue<T> right)
		{
			return Equals(left, right);
		}

		public static bool operator !=(TopicAndPartitionValue<T> left, TopicAndPartitionValue<T> right)
		{
			return !Equals(left, right);
		}

		public override string ToString()
		{
			return _topicAndPartition + ": " + _value;
		}
	}
}