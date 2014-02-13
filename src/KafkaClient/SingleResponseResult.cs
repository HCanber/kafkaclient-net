using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Kafka.Client
{
	public class SingleResponseResult<TResonse, TPayload>
	{
		private readonly TResonse _response;
		private readonly TopicAndPartitionValue<TPayload> _failedItem;

		public SingleResponseResult(TResonse response, TopicAndPartitionValue<TPayload> failedItem)
		{
			_response = response;
			_failedItem =failedItem;
		}

		public TResonse Response { get { return _response; } }

		public TopicAndPartitionValue<TPayload> FailedItem { get { return _failedItem; } }
	}
}