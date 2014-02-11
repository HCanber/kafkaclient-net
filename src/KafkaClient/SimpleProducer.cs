using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;

namespace Kafka.Client
{
	public class SimpleProducer : ProducerBase
	{
		public SimpleProducer(IKafkaClient client)
			: base(client)
		{
		}

		public ProducerResponseStatus Send(string topic, int partition, byte[] value, byte[] key = null)
		{
			IReadOnlyList<TopicAndPartitionValue<IEnumerable<IMessage>>> failedItems;
			var responses = base.SendProduce(new[]{new TopicAndPartitionValue<IEnumerable<IMessage>>(new TopicAndPartition(topic,partition),new[]{new Message(key,value)} ), },out failedItems);

			var produceResponse = responses.First();
			if(produceResponse.HasError)
				throw new ProduceFailedException(produceResponse.GetErrors());

			return produceResponse.StatusesByTopic[topic][0];
		}
	}	
}