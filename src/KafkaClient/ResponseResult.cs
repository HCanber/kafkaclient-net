using System.Collections.Generic;
using System.Collections.ObjectModel;
using Kafka.Client.Utils;

namespace Kafka.Client
{
	public class ResponseResult<TResonse, TPayload>
	{
		private readonly IReadOnlyCollection<TResonse> _responses;
		private readonly IReadOnlyList<TopicAndPartitionValue<TPayload>> _failedItems;

		public ResponseResult(IReadOnlyCollection<TResonse> responses, List<TopicAndPartitionValue<TPayload>> failedItems)
		{
			_responses = responses;
			_failedItems =new ReadOnlyCollection<TopicAndPartitionValue<TPayload>>(failedItems);
		}

		public IReadOnlyCollection<TResonse> Responses { get { return _responses; } }

		public IReadOnlyCollection<TopicAndPartitionValue<TPayload>> FailedItems { get { return _failedItems; } }
	}
}