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

		public ProducerBase(IKafkaClient client, bool allowTopicsToBeCreated=true)
		{
			if(client == null) throw new ArgumentNullException("client");
			_client = client;
			_allowTopicsToBeCreated = allowTopicsToBeCreated;
		}

		protected IKafkaClient Client { get { return _client; } }

		protected async Task<SingleResponseResult<ProduceResponse, IEnumerable<IMessage>>> SendProduceAsync(TopicAndPartition topicAndPartition, byte[] value, byte[] key, RequiredAck requiredAcks, int ackTimeoutMs, CancellationToken cancellationToken)
		{
			var messageForTopicAndPartition = new[] {new TopicAndPartitionValue<IEnumerable<IMessage>>(topicAndPartition, new[] {new Message(key, value)})};
			ResponseResult<ProduceResponse, IEnumerable<IMessage>> result;
			try
			{
				result = await SendProduceAsync(messageForTopicAndPartition,requiredAcks,ackTimeoutMs, cancellationToken);
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
			var produceResponse = new SingleResponseResult<ProduceResponse, IEnumerable<IMessage>>(result.Responses.FirstOrDefault(), result.FailedItems.FirstOrDefault());

			return produceResponse;
		}

		protected Task<ResponseResult<ProduceResponse, IEnumerable<IMessage>>> SendProduceAsync(IEnumerable<TopicAndPartitionValue<IEnumerable<IMessage>>> messages, RequiredAck requiredAcks, int ackTimeoutMs, CancellationToken cancellationToken)
		{
			var responses = Client.SendToLeaderAsync(messages, (items, requestid) =>
				new ProduceRequest(items, requiredAcks, ackTimeoutMs),
				ProduceResponse.Deserialize, cancellationToken, allowTopicsToBeCreated: _allowTopicsToBeCreated);

			return responses;

		}
	}
}