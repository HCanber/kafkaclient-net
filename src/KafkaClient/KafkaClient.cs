using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Kafka.Client.Api;
using Kafka.Client.Network;

namespace Kafka.Client
{
	public class KafkaClient : KafkaClientBase, IKafkaClient
	{
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();
		public const int DefaultBufferSize = AsyncKafkaConnection.UseDefaultBufferSize;
		public const int DefaultReadTimeoutMs = 120 * 1000;
		private readonly ConcurrentDictionary<HostPort, IAsyncKafkaConnection> _connectionsByHostPort = new ConcurrentDictionary<HostPort, IAsyncKafkaConnection>();

		private readonly HostPort _hostPort;
		private readonly string _clientId;
		private readonly int _readTimeoutMs;
		private readonly int _readBufferSize;
		private readonly int _writeBufferSize;
		private readonly TraceLogRequests _requestsToLog;
		private readonly MetadataHolder _metadata;

		public KafkaClient(string host, ushort port, string clientId, int readTimeoutMs = DefaultReadTimeoutMs, int readBufferSize = DefaultBufferSize, int writeBufferSize = DefaultBufferSize, TraceLogRequests requestsToLog = TraceLogRequests.None)
			: this(new HostPort(host, port), clientId, readTimeoutMs, readBufferSize, writeBufferSize, requestsToLog)
		{
			//Intentionally left blank
		}

		public KafkaClient(HostPort hostPort, string clientId, int readTimeoutMs = DefaultReadTimeoutMs, int readBufferSize = DefaultBufferSize, int writeBufferSize = DefaultBufferSize, TraceLogRequests requestsToLog=TraceLogRequests.None)
		{
			_hostPort = hostPort;
			_clientId = clientId;
			_readTimeoutMs = readTimeoutMs;
			_readBufferSize = readBufferSize;
			_writeBufferSize = writeBufferSize;
			_requestsToLog = requestsToLog;
			_metadata = new MetadataHolder(this);
		}

		public string ClientId { get { return _clientId; } }

		/// <summary>
		/// Connects this instance to the broker specified in the constructor. Calling this is optional. If not called, this instance will connect to the broker when needed.
		/// Calling Connect() manually gives you a chance to handle any exceptions related to the broker is not responding.
		/// </summary>
		public void Connect()
		{
			_connectionsByHostPort.TryAdd(_hostPort, CreateConnection(_hostPort, _readBufferSize, _writeBufferSize, _readTimeoutMs));
		}


		protected override string GetClientId()
		{
			return _clientId;
		}

		protected IAsyncKafkaConnection CreateConnection(HostPort hostPort)
		{
			return CreateConnection(hostPort, _readBufferSize, _writeBufferSize, _readTimeoutMs);
		}

		private IAsyncKafkaConnection CreateConnection(HostPort hostPort, int readBufferSize, int writeBufferSize, int readTimeoutMs)
		{
			var connection = new AsyncKafkaConnection(hostPort, readBufferSize, writeBufferSize, readTimeoutMs, autoConnect: true, requestsToLog: _requestsToLog);
			return connection;
		}

		protected override ILog Logger
		{
			get { return _Logger; }
		}

		protected override IEnumerable<IAsyncKafkaConnection> GetAllConnections()
		{
			var connections = _connectionsByHostPort.Values;
			return connections;
		}

		protected override IAsyncKafkaConnection GetConnectionForBroker(Broker broker)
		{
			var connection = _connectionsByHostPort.GetOrAdd(broker.Host,
				hostPort =>
				{
					_Logger.DebugFormat("No connection to broker {0} found. Creating new.", hostPort);
					return CreateConnection(hostPort);
				});
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

		public Task<IReadOnlyList<TopicMetadata>> GetRawMetadataForAllTopicsAsync(CancellationToken cancellationToken)
		{
			return _metadata.GetRawMetadataForTopicsAsync(null, cancellationToken, useCachedValues: false);
		}

		public Task<IReadOnlyList<TopicMetadata>> GetRawMetadataForTopicsAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken, bool useCachedValues = true)
		{
			return _metadata.GetRawMetadataForTopicsAsync(topics, cancellationToken, useCachedValues: useCachedValues);
		}

		public Task<TopicMetadata> GetMetadataForTopicAsync(string topic, CancellationToken cancellationToken, bool useCachedValues = true)
		{
			return _metadata.GetMetadataForTopic(topic, cancellationToken, useCachedValues: useCachedValues);
		}

		public Task<TopicMetadataResponse> SendMetadataRequestForTopicsAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken)
		{
			return SendMetadataRequestAsync(topics, cancellationToken);
		}

		public Task<ResponseResult<TResponse, TPayload>> SendToLeaderAsync<TPayload, TResponse>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads, RequestBuilder<TPayload> requestBuilder, ResponseDeserializer<TResponse> responseDeserializer, CancellationToken cancellationToken, bool allowTopicsToBeCreated = false)
		{
			return SendRequestToLeaderAsync(payloads, requestBuilder, responseDeserializer, allowTopicsToBeCreated, cancellationToken);
		}

		public Task<IReadOnlyList<TopicItem<IReadOnlyList<int>>>> GetPartitionsForTopicsAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken)
		{
			return _metadata.GetPartitionsForTopicsAsync(topics, cancellationToken);
		}



		protected override Task<Broker> GetLeaderAsync(TopicAndPartition topicAndPartition, bool allowTopicsToBeCreated, CancellationToken cancellationToken)
		{
			return _metadata.GetLeaderAsync(topicAndPartition, cancellationToken, allowTopicsToBeCreated: allowTopicsToBeCreated);
		}

		public void Close()
		{
			CloseAllConnections();
		}
	}
}