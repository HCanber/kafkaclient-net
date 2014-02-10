using System.Collections.Generic;
using Kafka.Client.Api;
using Kafka.Client.IO;

namespace Kafka.Client
{
	public delegate IKafkaRequest RequestBuilder<T>(IReadOnlyCollection<TopicAndPartitionValue<T>> items, int requestId);

	public delegate TRequest ResponseDeserializer<out TRequest>(IReadBuffer response);

	public interface IKafkaClient
	{
		string ClientId { get; }
		void ResetAllMetadata();
		void ResetMetadataForTopic(string topic);


		IReadOnlyCollection<TResponse> SendToLeader<TPayload, TResponse>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads, RequestBuilder<TPayload> requestBuilder, ResponseDeserializer<TResponse> responseDeserializer, out IReadOnlyCollection<TopicAndPartitionValue<TPayload>> failedItems);
		IReadOnlyCollection<TopicMetadata> GetMetadataForTopics(IReadOnlyCollection<string> topics);

		IReadOnlyCollection<KeyValuePair<string, IReadOnlyCollection<int>>> GetPartitionsForTopics(IReadOnlyCollection<string> topics);
	}
}