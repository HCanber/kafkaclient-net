using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class LeaderNotAvailableException : KafkaException
	{
		private readonly string _messageFormat;
		private readonly IReadOnlyCollection<TopicAndPartition> _topicAndPartitions;

		public LeaderNotAvailableException(TopicAndPartition topicAndPartition)
			: this(new ReadOnlyCollection<TopicAndPartition>(new[] {topicAndPartition}))
		{
			//Intentionally left blank
		}

		public LeaderNotAvailableException(IReadOnlyCollection<TopicAndPartition> topicAndPartitions)
			: this(topicAndPartitions, "Leader not available for {0}")
		{
			_topicAndPartitions = topicAndPartitions;
		}

		protected LeaderNotAvailableException(IReadOnlyCollection<TopicAndPartition> topicAndPartitions, string messageFormat)
		{
			_messageFormat = messageFormat;
		}


		protected LeaderNotAvailableException(SerializationInfo info, StreamingContext context) : base(info, context) { }

		public IReadOnlyCollection<TopicAndPartition> TopicAndPartitions
		{
			get { return _topicAndPartitions; }
		}

		public override string ToString()
		{
			return string.Format(_messageFormat, string.Join(", ",_topicAndPartitions));
		}
	}
}