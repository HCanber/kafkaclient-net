using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Kafka.Client.Utils;

namespace Kafka.Client
{
	public class ResponseResult<TResonse, TPayload>
	{
		private readonly IReadOnlyCollection<TResonse> _responses;
		private readonly ReadOnlyCollection<Tuple<TopicAndPartitionValue<TPayload>, Exception>> _failedItems;

		public ResponseResult(IReadOnlyCollection<TResonse> responses, List<Tuple<TopicAndPartitionValue<TPayload>,Exception>> failedItems)
		{
			_responses = responses;
			_failedItems = new ReadOnlyCollection<Tuple<TopicAndPartitionValue<TPayload>, Exception>>(failedItems);
		}

		public IReadOnlyCollection<TResonse> Responses { get { return _responses; } }

		public ReadOnlyCollection<Tuple<TopicAndPartitionValue<TPayload>, Exception>> FailedItems { get { return _failedItems; } }
	}
}