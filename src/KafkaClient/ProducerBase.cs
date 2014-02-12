using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;

namespace Kafka.Client
{
	public abstract class ProducerBase
	{
		private readonly IKafkaClient _client;
		private readonly bool _allowTopicsToBeCreated;

		public ProducerBase(IKafkaClient client, bool allowTopicsToBeCreated=true)
		{
			if(client == null) throw new ArgumentNullException("client");
			_client = client;
			_allowTopicsToBeCreated = allowTopicsToBeCreated;
		}

		protected IKafkaClient Client { get { return _client; } }

		protected ProduceResponse SendProduce(TopicAndPartition topicAndPartition, byte[] value, byte[] key, out IReadOnlyList<TopicAndPartitionValue<IEnumerable<IMessage>>> failedItems, RequiredAck requiredAcks, int ackTimeoutMs)
		{
			var messageForTopicAndPartition = new[] {new TopicAndPartitionValue<IEnumerable<IMessage>>(topicAndPartition, new[] {new Message(key, value)})};
			IReadOnlyCollection<ProduceResponse> responses;
			try
			{
				responses = SendProduce(messageForTopicAndPartition, out failedItems,requiredAcks,ackTimeoutMs);
			}
			catch(KafkaMetaDataException e)
			{
				if(e.TopicErrors.Count == 1 && e.TopicErrors[0].Item == KafkaError.LeaderNotAvailable)
				{
					//This error code is sent as an error on the topic level when the topic did not exist, but config has autoCreateTopicsEnable=true.
					//So the topic is created but no leader is known at this time.
					throw new TopicCreatedNoLeaderYetException(topicAndPartition);
				}
				throw;
			}
			var produceResponse = responses.First();

			return produceResponse;
		}

		protected IReadOnlyCollection<ProduceResponse> SendProduce(IEnumerable<TopicAndPartitionValue<IEnumerable<IMessage>>> messages, out IReadOnlyList<TopicAndPartitionValue<IEnumerable<IMessage>>> failedItems, RequiredAck requiredAcks,int ackTimeoutMs)
		{
			var responses = Client.SendToLeader(messages, (items, requestid) =>
				new ProduceRequest(items, requiredAcks, ackTimeoutMs),
				ProduceResponse.Deserialize, out failedItems, _allowTopicsToBeCreated);

			return responses;

		}
	}
}