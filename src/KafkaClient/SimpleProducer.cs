using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;

namespace Kafka.Client
{
	public class SimpleProducer : ProducerBase
	{
		private readonly RequiredAck _requiredAcks;
		private readonly int _ackTimeoutMs;

		public SimpleProducer(IKafkaClient client, RequiredAck requiredAcks = RequiredAck.WrittenToDiskByLeader, int ackTimeoutMs = 1000)
			: base(client)
		{
			_requiredAcks = requiredAcks;
			_ackTimeoutMs = ackTimeoutMs;
		}

		public ProducerResponseStatus Send(string topic, int partition, byte[] value, byte[] key = null)
		{
			return SendAsync(topic, partition, value, key).Result;
		}

		public async Task<ProducerResponseStatus> SendAsync(string topic, int partition, byte[] value, byte[] key = null)
		{
			return await SendAsync(topic, partition, value, CancellationToken.None, key);
		}

		public async Task<ProducerResponseStatus> SendAsync(string topic, int partition, byte[] value, CancellationToken cancellationToken, byte[] key = null)
		{
			var topicAndPartition = new TopicAndPartition(topic, partition);

			ProduceResponse response = null;
			var attempt = 1;
			while(response == null && attempt <= 3)
			{
				try
				{
					var result = await SendProduceAsync(topicAndPartition, value, key, _requiredAcks, _ackTimeoutMs, cancellationToken);
					response = result.Response;
					var failedItem = result.FailedItem;
					if(failedItem != null) throw new ProduceFailedException("Unexpected error occurred.");
					if(response.HasError)
						throw new ProduceFailedException(response.GetErrors());
					return response.StatusesByTopic[topic][0];
				}
				catch(TopicCreatedNoLeaderYetException)
				{
					Thread.Sleep(attempt * 200);
				}
				attempt++;
			}
			throw new LeaderNotAvailableException(topicAndPartition);
		}
	}
}