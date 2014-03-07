using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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


		Task<ResponseResult<TResponse, TPayload>> SendToLeaderAsync<TPayload, TResponse>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads, RequestBuilder<TPayload> requestBuilder, ResponseDeserializer<TResponse> responseDeserializer, CancellationToken cancellationToken, bool allowTopicsToBeCreated = false);


		/// <summary>
		/// Gets metadata for the topic.
		/// </summary>
		/// <param name="topic">The topic.</param>
		/// <param name="cancellationToken"></param>
		/// <param name="useCachedValues">if set to <c>true</c> use cached values; otherwise the meta data is fetched from the server.</param>
		/// <exception cref="UnknownTopicException">Thrown if the topic do not exist</exception>
		/// <exception cref="TopicCreatedNoLeaderYetException">Thrown if the topic did not exist, but was created. 
		/// You should retry in a while when the new topic has been propagated to other brokers, and a leader has been elected.</exception>
		Task<TopicMetadata> GetMetadataForTopicAsync([NotNull] string topic, CancellationToken cancellationToken, bool useCachedValues = true);

		///  <summary>This will get meta data for all topics, without any exceptions being thrown. </summary>
		/// <param name="topics">The topics. If <c>null</c> then </param>
		/// <param name="cancellationToken"></param>
		/// <param name="useCachedValues">When <c>true</c> uses cached values; otherwise all meta data is fetched from the server.</param>
		Task<IReadOnlyList<TopicMetadata>> GetRawMetadataForTopicsAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken, bool useCachedValues = true);

		Task<IReadOnlyList<TopicItem<IReadOnlyList<int>>>> GetPartitionsForTopicsAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken);
		void Close();
	}
}