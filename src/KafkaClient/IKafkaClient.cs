using System.Collections.Generic;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;
using Kafka.Client.IO;
using Kafka.Client.JetBrainsAnnotations;

namespace Kafka.Client
{
	public delegate IKafkaRequest RequestBuilder<T>(IReadOnlyCollection<TopicAndPartitionValue<T>> items, int requestId);

	public delegate TRequest ResponseDeserializer<out TRequest>(IReadBuffer response);

	public interface IKafkaClient
	{
		string ClientId { get; }
		void ResetAllMetadata();
		void ResetMetadataForTopic(string topic);


		IReadOnlyList<TResponse> SendToLeader<TPayload, TResponse>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads, RequestBuilder<TPayload> requestBuilder, ResponseDeserializer<TResponse> responseDeserializer, out IReadOnlyList<TopicAndPartitionValue<TPayload>> failedItems, bool allowTopicsToBeCreated=false);


		/// <summary>
		/// Gets metadata for the topic.
		/// </summary>
		/// <param name="topic">The topic.</param>
		/// <param name="useCachedValues">if set to <c>true</c> use cached values; otherwise the meta data is fetched from the server.</param>
		/// <exception cref="UnknownTopicException">Thrown if the topic do not exist</exception>
		/// <exception cref="TopicCreatedNoLeaderYetException">Thrown if the topic did not exist, but was created. 
		/// You should retry in a while when the new topic has been propagated to other brokers, and a leader has been elected.</exception>
		TopicMetadata GetMetadataForTopic([NotNull]string topic, bool useCachedValues = true);

		///  <summary>This will get meta data for all topics, without any exceptions being thrown. </summary>
		/// <param name="topics">The topics. If <c>null</c> then </param>
		/// <param name="useCachedValues">When <c>true</c> uses cached values; otherwise all meta data is fetched from the server.</param>
		IReadOnlyList<TopicMetadata> GetRawMetadataForTopics(IReadOnlyCollection<string> topics, bool useCachedValues=true);

		IReadOnlyList<TopicItem<IReadOnlyList<int>>> GetPartitionsForTopics(IReadOnlyCollection<string> topics);
	}
}