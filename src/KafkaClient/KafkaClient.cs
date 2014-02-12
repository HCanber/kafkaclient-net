using System.Collections.Concurrent;
using System.Collections.Generic;
using Common.Logging;
using Kafka.Client.Api;
using Kafka.Client.Network;

namespace Kafka.Client
{
	public class KafkaClient : KafkaClientBase, IKafkaClient
	{
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();
		public const int DefaultBufferSize = KafkaConnection.UseDefaultBufferSize;
		public const int DefaultReadTimeoutMs = 120 * 1000;
		private readonly ConcurrentDictionary<HostPort, IKafkaConnection> _connectionsByHostPort = new ConcurrentDictionary<HostPort, IKafkaConnection>();

		private readonly string _clientId;
		private readonly int _readTimeoutMs;
		private readonly int _readBufferSize;
		private readonly int _writeBufferSize;
		private readonly MetadataHolder _metadata;

		public KafkaClient(string host, ushort port, string clientId, int readTimeoutMs = DefaultReadTimeoutMs, int readBufferSize = DefaultBufferSize, int writeBufferSize = DefaultBufferSize)
			: this(new HostPort(host, port), clientId, readTimeoutMs, readBufferSize, writeBufferSize)
		{
			//Intentionally left blank
		}

		public KafkaClient(HostPort hostPort, string clientId, int readTimeoutMs = DefaultReadTimeoutMs, int readBufferSize = DefaultBufferSize, int writeBufferSize = DefaultBufferSize)
		{
			_clientId = clientId;
			_readTimeoutMs = readTimeoutMs;
			_readBufferSize = readBufferSize;
			_writeBufferSize = writeBufferSize;
			_connectionsByHostPort.TryAdd(hostPort, CreateConnection(hostPort, _readBufferSize, _writeBufferSize, _readTimeoutMs));
			_metadata = new MetadataHolder(this);
		}

		public string ClientId { get { return _clientId; } }


		protected override string GetClientId()
		{
			return _clientId;
		}

		protected IKafkaConnection CreateConnection(HostPort hostPort)
		{
			return CreateConnection(hostPort, _readBufferSize, _writeBufferSize, _readTimeoutMs);
		}

		private static IKafkaConnection CreateConnection(HostPort hostPort, int readBufferSize, int writeBufferSize, int readTimeoutMs)
		{
			return new KafkaConnection(hostPort, readBufferSize, writeBufferSize, readTimeoutMs);
		}

		protected override ILog Logger
		{
			get { return _Logger; }
		}

		protected override IEnumerable<IKafkaConnection> GetAllConnections()
		{
			var connections = _connectionsByHostPort.Values;
			return connections;
		}

		protected override IKafkaConnection GetConnectionForBroker(Broker broker)
		{
			var connection = _connectionsByHostPort.GetOrAdd(broker.Host, CreateConnection);
			return connection;
		}

		public void ResetAllMetadata()
		{
			_metadata.ResetAllMetadata();
		}

		public void ResetMetadataForTopic(string topic)
		{
			_metadata.ResetMetadataForTopic(topic);
		}

		public IReadOnlyList<TopicMetadata> GetRawMetadataForAllTopics()
		{
			return _metadata.GetRawMetadataForTopics(null,false);
		}

		public IReadOnlyList<TopicMetadata> GetRawMetadataForTopics(IReadOnlyCollection<string> topics, bool useCachedValues=true)
		{
			return _metadata.GetRawMetadataForTopics(topics,useCachedValues);
		}

		public TopicMetadata GetMetadataForTopic(string topic, bool useCachedValues = true)
		{
			return _metadata.GetMetadataForTopic(topic,useCachedValues);
		}

		public TopicMetadataResponse SendMetadataRequestForTopics(IReadOnlyCollection<string> topics)
		{
			return SendMetadataRequest(topics);
		}

		public IReadOnlyList<TResponse> SendToLeader<TPayload, TResponse>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads, RequestBuilder<TPayload> requestBuilder, ResponseDeserializer<TResponse> responseDeserializer, out IReadOnlyList<TopicAndPartitionValue<TPayload>> failedItems, bool allowTopicsToBeCreated=false)
		{
			return SendRequestToLeader(payloads, requestBuilder, responseDeserializer, out failedItems, allowTopicsToBeCreated);
		}

		public IReadOnlyList<TopicItem<IReadOnlyList<int>>> GetPartitionsForTopics(IReadOnlyCollection<string> topics)
		{
			return _metadata.GetPartitionsForTopics(topics);
		}



		protected override Broker GetLeader(TopicAndPartition topicAndPartition, bool allowTopicsToBeCreated)
		{
			return _metadata.GetLeader(topicAndPartition,allowTopicsToBeCreated);
		}

		public void Close()
		{
			CloseAllConnections();
		}
	}
}