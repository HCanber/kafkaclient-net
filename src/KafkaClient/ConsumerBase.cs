using System;
using System.Collections.Generic;
using System.Linq;
using Kafka.Client.Api;

namespace Kafka.Client
{

	//public class KafkaClient
	//{
	//	private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();
	//	public const string DefaultClientId = "kafka-net";
	//	public const int DefaultBufferSize =  -1;
	//	public const int DefaultReadTimeoutMs =  120*1000;

	//	private readonly string _clientId;
	//	private readonly int _readTimeoutMs;
	//	private readonly int _readBufferSize;
	//	private readonly int _writeBufferSize;
	//	private readonly ConcurrentDictionary<HostPort, KafkaConnection> _connectionsByHostPort=new ConcurrentDictionary<HostPort, KafkaConnection>();
	//	private readonly ConcurrentDictionary<string, ConcurrentSet<int>> _partitionsByTopic = new ConcurrentDictionary<string, ConcurrentSet<int>>();
	//	private readonly ConcurrentDictionary<TopicAndPartition, Broker> _leaderForTopicAndPartition = new ConcurrentDictionary<TopicAndPartition, Broker>(); 
	//	private int _requestId=0;


	//	public KafkaClient(string host, ushort port, string clientId=DefaultClientId, int readTimeoutMs=DefaultReadTimeoutMs,int readBufferSize=DefaultBufferSize, int writeBufferSize=DefaultBufferSize)
	//		:this(new HostPort(host,port),clientId,readTimeoutMs,readBufferSize,writeBufferSize)
	//	{
	//		//Intentionally left blank
	//	}

	//	public KafkaClient(HostPort hostPort, string clientId = DefaultClientId, int readTimeoutMs = DefaultReadTimeoutMs, int readBufferSize = DefaultBufferSize, int writeBufferSize = DefaultBufferSize)
	//	{
	//		_clientId = clientId;
	//		_readTimeoutMs = readTimeoutMs;
	//		_readBufferSize = readBufferSize;
	//		_writeBufferSize = writeBufferSize;
	//		_connectionsByHostPort.TryAdd(hostPort, CreateKafkaConnection(hostPort));			
	//	}

	//	public string ClientId { get { return _clientId; } }

	//	public void Close()
	//	{
	//		var connections = _connectionsByHostPort.Values;
	//		connections.ForEach(ch => ch.Disconnect());
	//	}


	//	public TopicMetadataResponse GetMetadataForAllTopics()
	//	{
	//		return GetMetadataForTopics(new string[0]);
	//	}

	//	public TopicMetadataResponse GetMetadataForTopics(IReadOnlyCollection<string> topics)
	//	{
	//		var requestId = GetNextRequestId();
	//		var request = new TopicMetadataRequest(topics);
	//		var response = SendToAnyBroker(request, TopicMetadataResponse.Deserialize, requestId);
	//		return response;
	//	}


	//	public void ResetAllMetadata()
	//	{
	//		_partitionsByTopic.Clear();
	//		_leaderForTopicAndPartition.Clear();
	//	}


	//	public void ResetMetadataForTopic(string topic)
	//	{
	//		ConcurrentSet<int> partitions;
	//		if(_partitionsByTopic.TryRemove(topic, out partitions))
	//		{
	//			foreach(var partition in partitions)
	//			{
	//				Broker leader;
	//				_leaderForTopicAndPartition.TryRemove(new TopicAndPartition(topic, partition), out leader);
	//			}
	//		}
	//	}


	//	public int GetNextRequestId()
	//	{
	//		return Interlocked.Increment(ref _requestId);
	//	}


	//	private TResponse SendToAnyBroker<TResponse>(IKafkaRequest message, Func<IReadBuffer, TResponse> deserializer, int requestId)
	//	{
	//		foreach(var connection in _connectionsByHostPort.Values)
	//		{
	//			try
	//			{
	//				var response = SendAndReceive(connection, message, deserializer, requestId);
	//				return response;
	//			}
	//			catch(Exception ex)
	//			{
	//				_Logger.WarnException(ex, "Error while communicating with server {1} for request {0}. Trying next.", message, connection);
	//			}
	//		}
	//		var errorMessage = string.Format("Could not send request {0} to any server", message);
	//		_Logger.ErrorFormat(errorMessage);
	//		throw new KafkaException(errorMessage);
	//	}

	//	private TResponse SendAndReceive<TResponse>(KafkaConnection connection, IKafkaRequest message, Func<IReadBuffer, TResponse> deserializer, int requestId)
	//	{
	//		lock(connection)
	//		{
	//			var responseBytes = connection.Request(new KafkaRequestWriteable(message,_clientId), requestId);
	//			var response = Deserialize(deserializer, responseBytes);
	//			return response;
	//		}
	//	}

	//	public List<TResponse> SendToLeader<T, TResponse>(IEnumerable<PayloadForTopicAndPartition<T>> payloads, PayloadToMessageConverter<T> payloadToMessageConverter, Func<IReadBuffer, TResponse> deserializer, out IReadOnlyCollection<PayloadForTopicAndPartition<T>> failedItems)
	//	{
	//		var payloadsByBroker = new Dictionary<Broker, List<PayloadForTopicAndPartition<T>>>();
	//		foreach(var payload in payloads)
	//		{
	//			var topicAndPartition=payload.TopicAndPartition;
	//			var leader = GetLeader(topicAndPartition);
	//			if(leader == null) throw new PartitionUnavailableException(topicAndPartition, "No leader for " + topicAndPartition);
	//			payloadsByBroker.AddToList(leader, payload);
	//		}

	//		var failed = new List<PayloadForTopicAndPartition<T>>();
	//		var responses = new List<TResponse>();
	//		foreach(var kvp in payloadsByBroker)
	//		{
	//			var broker = kvp.Key;
	//			var payloadsForBroker= kvp.Value;
	//			var requestId = GetNextRequestId();
	//			var message = payloadToMessageConverter(payloadsForBroker,requestId);
	//			var connection = GetConnectionForBroker(broker);
	//			try
	//			{
	//				var response = SendAndReceive(connection, message, deserializer, requestId);
	//				responses.Add(response);
	//			}
	//			catch(Exception ex)
	//			{
	//				failed.AddRange(payloadsForBroker);
	//				_Logger.WarnException(ex,"Error while sending request {0} to server {1}",message, connection);
	//			}
	//		}
	//		failedItems = failed;
	//		return responses;
	//	}


	//	protected Broker GetLeader(TopicAndPartition topicAndPartition)
	//	{
	//		var topic=topicAndPartition.Topic;
	//		Broker broker;
	//		if(!_leaderForTopicAndPartition.TryGetValue(topicAndPartition, out broker))
	//		{
	//			LoadMetaForTopics(new[] {topic});

	//			if(!_leaderForTopicAndPartition.TryGetValue(topicAndPartition, out broker))
	//			{
	//				throw new KafkaInvalidPartitionException(topicAndPartition, string.Format("Partition{0} do not exist.", topicAndPartition));
	//			}
	//		}
	//		return broker;
	//	}


	//	private void LoadMetaForTopics(IReadOnlyCollection<string> topics=null)
	//	{
	//		var response = GetMetadataForTopics(topics ?? new string[0]);
	//		foreach(var topicMetadata in response.TopicMetadatas)
	//		{
	//			var topic = topicMetadata.Topic;
	//			ResetMetadataForTopic(topic);
	//			if(topicMetadata.PartionMetaDatas.Count > 0)
	//			{
	//				var partitions = _partitionsByTopic.GetOrAdd(topic, _ => new ConcurrentSet<int>());
	//				foreach(var partitionMetadata in topicMetadata.PartionMetaDatas)
	//				{
	//					var partitionId = partitionMetadata.PartitionId;
	//					var topicAndPartition = new TopicAndPartition(topic, partitionId);
	//					_leaderForTopicAndPartition.TryAdd(topicAndPartition, partitionMetadata.Leader);
	//					partitions.TryAdd(partitionId);
	//				}
	//			}
	//		}
	//	}

	//	private static TResponse Deserialize<TResponse>(Func<IReadBuffer, TResponse> deserializer, byte[] response)
	//	{
	//		var readBuffer = new ReadBuffer(response);
	//		var deserialized = deserializer(readBuffer);
	//		return deserialized;
	//	}

	//	private KafkaConnection GetConnectionForBroker(Broker broker)
	//	{
	//		var connection = _connectionsByHostPort.GetOrAdd(broker.Host,CreateKafkaConnection);
	//		return connection;
	//	}

	//	private KafkaConnection CreateKafkaConnection(HostPort hostPort)
	//	{
	//		return new KafkaConnection(hostPort, _readBufferSize, _writeBufferSize, _readTimeoutMs);
	//	}
	//}

	public abstract class ConsumerBase
	{
		private readonly IKafkaClient _client;

		public ConsumerBase(IKafkaClient client)
		{
			if(client == null) throw new ArgumentNullException("client");
			_client = client;
		}

		protected IKafkaClient Client { get { return _client; } }


		protected IReadOnlyCollection<FetchResponse> FetchMessages(string topic, IEnumerable<KeyValuePair<int, long>> partitionAndOffsets, out IReadOnlyCollection<PayloadForTopicAndPartition<long>> failedItems, int fetchMaxBytes, int minBytes = 1, int maxWaitForMessagesInMs = 100)
		{
			return FetchMessages(partitionAndOffsets.Select(p => new PayloadForTopicAndPartition<long>(new TopicAndPartition(topic, p.Key), p.Value)).ToList(), out failedItems, fetchMaxBytes, minBytes, maxWaitForMessagesInMs);
		}

		protected IReadOnlyCollection<FetchResponse> FetchMessages(TopicAndPartition topic, long offset, out IReadOnlyCollection<PayloadForTopicAndPartition<long>> failedItems, int fetchMaxBytes, int minBytes = 1, int maxWaitForMessagesInMs = 100)
		{
			return FetchMessages(new[] { new PayloadForTopicAndPartition<long>(topic, offset) }, out failedItems, fetchMaxBytes, minBytes, maxWaitForMessagesInMs);
		}

		protected IReadOnlyCollection<FetchResponse> FetchMessages(IReadOnlyCollection<PayloadForTopicAndPartition<long>> topicsAndOffsets, out IReadOnlyCollection<PayloadForTopicAndPartition<long>> failedItems, int fetchMaxBytes, int minBytes = FetchRequest.DefaultMinBytes, int maxWaitForMessagesInMs = FetchRequest.DefaultMaxWait)
		{
			if(fetchMaxBytes<MessageSetItem.SmallestPossibleSize) 
				throw new ArgumentException("maxBytes must be at least "+MessageSetItem.SmallestPossibleSize,"fetchMaxBytes");

			var responses = _client.SendToLeader(topicsAndOffsets, (items, reqId) => new FetchRequest(items.Select(t => new KeyValuePair<TopicAndPartition, PartitionFetchInfo>(t.TopicAndPartition, new PartitionFetchInfo(t.Payload, fetchMaxBytes))), minBytes, maxWaitForMessagesInMs), FetchResponse.Deserialize, out failedItems);
			return responses;
		}


	}
}