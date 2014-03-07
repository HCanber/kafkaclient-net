using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;
using Kafka.Client.Utils;

namespace Kafka.Client
{
	public abstract class ProducerBase
	{
		private readonly IKafkaClient _client;
		private readonly bool _allowTopicsToBeCreated;

		public ProducerBase(IKafkaClient client, bool allowTopicsToBeCreated = true)
		{
			if(client == null) throw new ArgumentNullException("client");
			_client = client;
			_allowTopicsToBeCreated = allowTopicsToBeCreated;
		}

		protected IKafkaClient Client { get { return _client; } }

		protected Task<ResponseResult<ProduceResponse, IEnumerable<IMessage>>> SendProduceAsync(IEnumerable<TopicAndPartitionValue<IEnumerable<IMessage>>> messages, RequiredAck requiredAcks, int ackTimeoutMs, CancellationToken cancellationToken)
		{
			var responses = Client.SendToLeaderAsync(messages, (items, requestid) =>
				new ProduceRequest(items, requiredAcks, ackTimeoutMs),
				ProduceResponse.Deserialize, cancellationToken, allowTopicsToBeCreated: _allowTopicsToBeCreated);

			return responses;
		}
	}
}