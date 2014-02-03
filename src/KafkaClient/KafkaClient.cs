using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Common.Logging;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;
using Kafka.Client.Network;
using Kafka.Client.Utils;

namespace Kafka.Client
{
	public class KafkaClient : KafkaClientBase, IKafkaClient
	{
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();
		public const int DefaultBufferSize = KafkaConnection.UseDefaultBufferSize;
		public const int DefaultReadTimeoutMs = 120 * 1000;

		private readonly ConcurrentDictionary<HostPort, IKafkaConnection> _connectionsByHostPort = new ConcurrentDictionary<HostPort, IKafkaConnection>();
		private readonly ConcurrentDictionary<string, ConcurrentSet<int>> _partitionsByTopic = new ConcurrentDictionary<string, ConcurrentSet<int>>();
		private readonly ConcurrentDictionary<TopicAndPartition, Broker> _leaderForTopicAndPartition = new ConcurrentDictionary<TopicAndPartition, Broker>();

		private readonly string _clientId;
		private readonly int _readTimeoutMs;
		private readonly int _readBufferSize;
		private readonly int _writeBufferSize;

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
			_partitionsByTopic.Clear();
			_leaderForTopicAndPartition.Clear();
		}

		public void ResetMetadataForTopic(string topic)
		{
			ConcurrentSet<int> partitions;
			if(_partitionsByTopic.TryRemove(topic, out partitions))
			{
				foreach(var partition in partitions)
				{
					Broker leader;
					_leaderForTopicAndPartition.TryRemove(new TopicAndPartition(topic, partition), out leader);
				}
			}
		}

		public IReadOnlyCollection<TopicMetadata> GetMetadataForAllTopics()
		{
			return GetMetadataForTopics(null);
		}

		public IReadOnlyCollection<TopicMetadata> GetMetadataForTopics(IReadOnlyCollection<string> topics)
		{
			return SendMetadataRequest(topics).TopicMetadatas;
		}

		public IReadOnlyCollection<TResponse> SendToLeader<TPayload, TResponse>(IEnumerable<PayloadForTopicAndPartition<TPayload>> payloads, RequestBuilder<TPayload> requestBuilder, ResponseDeserializer<TResponse> responseDeserializer, out IReadOnlyCollection<PayloadForTopicAndPartition<TPayload>> failedItems)
		{
			return SendRequestToLeader(payloads, requestBuilder, responseDeserializer, out failedItems);
		}

		public IReadOnlyCollection<KeyValuePair<string, IReadOnlyCollection<int>>> GetPartitionsForTopics(IReadOnlyCollection<string> topics)
		{
			var partitionsByTopic = new List<KeyValuePair<string, IReadOnlyCollection<int>>>();
			Func<IEnumerable<string>, List<string>> handleTopics = (topcs) =>
			{
				var missing = new List<string>();
				foreach(var topic in topcs)
				{
					ConcurrentSet<int> ptns;
					if(_partitionsByTopic.TryGetValue(topic, out ptns))
						partitionsByTopic.Add(new KeyValuePair<string, IReadOnlyCollection<int>>(topic, ptns.ToImmutable()));
					else
						missing.Add(topic);
				}
				return missing;
			};
			var topicsMissing = handleTopics(topics);
			LoadMetaForTopics(topicsMissing);
			var stillMissingTopics = handleTopics(topicsMissing);
			if(stillMissingTopics.Count > 0)
			{
				throw new KafkaInvalidTopicException(stillMissingTopics, "Unknown topics: " + string.Join(", ", topics));
			}
			return partitionsByTopic;
		}

		private void LoadMetaForTopics(IReadOnlyCollection<string> topics = null)
		{
			var response = SendMetadataRequest(topics ?? new string[0]);
			foreach(var topicMetadata in response.TopicMetadatas)
			{
				var topic = topicMetadata.Topic;
				ResetMetadataForTopic(topic);
				if(topicMetadata.PartionMetaDatas.Count > 0)
				{
					var partitions = _partitionsByTopic.GetOrAdd(topic, _ => new ConcurrentSet<int>());
					foreach(var partitionMetadata in topicMetadata.PartionMetaDatas)
					{
						var partitionId = partitionMetadata.PartitionId;
						var topicAndPartition = new TopicAndPartition(topic, partitionId);
						_leaderForTopicAndPartition.TryAdd(topicAndPartition, partitionMetadata.Leader);
						partitions.TryAdd(partitionId);
					}
				}
			}
		}

		protected override Broker GetLeader(TopicAndPartition topicAndPartition)
		{
			var topic = topicAndPartition.Topic;
			Broker broker;
			if(!_leaderForTopicAndPartition.TryGetValue(topicAndPartition, out broker))
			{
				LoadMetaForTopics(new[] { topic });

				if(!_leaderForTopicAndPartition.TryGetValue(topicAndPartition, out broker))
				{
					throw new KafkaInvalidPartitionException(topicAndPartition, string.Format("Partition{0} do not exist.", topicAndPartition));
				}
			}
			return broker;
		}

		public void Close()
		{
			CloseAllConnections();
		}
	}
}