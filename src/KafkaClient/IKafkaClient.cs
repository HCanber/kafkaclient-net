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


		IReadOnlyList<TResponse> SendToLeader<TPayload, TResponse>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads, RequestBuilder<TPayload> requestBuilder, ResponseDeserializer<TResponse> responseDeserializer, out IReadOnlyList<TopicAndPartitionValue<TPayload>> failedItems);
		IReadOnlyList<TopicMetadata> GetMetadataForTopics(IReadOnlyCollection<string> topics);

		IReadOnlyList<KeyValuePair<string, IReadOnlyList<int>>> GetPartitionsForTopics(IReadOnlyCollection<string> topics);
	}
}