using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Api;

namespace Kafka.Client
{
	public abstract class ProducerBase
	{
		private readonly IKafkaClient _client;

		public ProducerBase(IKafkaClient client)
		{
			if(client == null) throw new ArgumentNullException("client");
			_client = client;
		}

		protected IKafkaClient Client { get { return _client; } }


		protected IReadOnlyCollection<ProduceResponse> SendProduce(IEnumerable<TopicAndPartitionValue<IEnumerable<IMessage>>> messages, out IReadOnlyCollection<TopicAndPartitionValue<IEnumerable<IMessage>>> failedItems, RequiredAck requiredAcks = RequiredAck.WrittenToDiskByLeader, int ackTimeoutMs = 1000)
		{
			var responses = Client.SendToLeader(messages, (items, requestid) =>
				new ProduceRequest(items, requiredAcks, ackTimeoutMs),
				ProduceResponse.Deserialize, out failedItems);

			return responses;
		}
	}
}