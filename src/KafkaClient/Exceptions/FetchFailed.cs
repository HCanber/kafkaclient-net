using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	public class FetchFailed : KafkaException
	{
		private readonly IReadOnlyCollection<Tuple<TopicAndPartition, FetchResponsePartitionData>> _errorResponses;

		protected FetchFailed()
		{
		}

		public FetchFailed(IReadOnlyCollection<Tuple<TopicAndPartition,FetchResponsePartitionData>> errorResponses, string message)
			: base(message)
		{
			_errorResponses = errorResponses;
		}

		public FetchFailed(IReadOnlyCollection<Tuple<TopicAndPartition,FetchResponsePartitionData>> errorResponses,string message, Exception inner)
			: base(message, inner)
		{
		}

		protected FetchFailed(
			SerializationInfo info,
			StreamingContext context)
			: base(info, context)
		{
		}

		public IReadOnlyCollection<Tuple<TopicAndPartition,FetchResponsePartitionData>> ErrorResponses
		{
			get { return _errorResponses; }
		}
	}
}