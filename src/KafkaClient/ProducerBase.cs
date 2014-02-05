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

		protected IReadOnlyCollection<ProduceResponse> Send(IEnumerable<KeyValuePair<TopicAndPartition, IEnumerable<IMessage>>> messages, IReadOnlyCollection<PayloadForTopicAndPartition<IEnumerable<IMessage>>> failedItems, RequiredAck requiredAcks = RequiredAck.WrittenToDiskByLeader, int ackTimeoutMs = 1000)
		{
			return Send(messages.Select(kvp => new PayloadForTopicAndPartition<IEnumerable<IMessage>>(kvp.Key, kvp.Value)), out failedItems, requiredAcks, ackTimeoutMs);
		}

		protected IReadOnlyCollection<ProduceResponse> Send(IEnumerable<PayloadForTopicAndPartition<IEnumerable<IMessage>>> messages, out IReadOnlyCollection<PayloadForTopicAndPartition<IEnumerable<IMessage>>> failedItems, RequiredAck requiredAcks = RequiredAck.WrittenToDiskByLeader, int ackTimeoutMs = 1000)
		{
			var responses = Client.SendToLeader(messages, (items, requestid) =>
				new ProduceRequest(items.Select(i => new KeyValuePair<TopicAndPartition, IEnumerable<IMessage>>(i.TopicAndPartition, i.Payload)), requiredAcks, ackTimeoutMs), 
				ProduceResponse.Deserialize, out failedItems);

			return responses;
		}
	}
}