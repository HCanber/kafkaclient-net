using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;
using Kafka.Client.IO;
using Kafka.Client.Network;
using Kafka.Client.Utils;

namespace Kafka.Client
{
	public abstract class KafkaClientBase
	{
		private int _requestId;


		/// <summary> Gets the logger for this instance.</summary>
		protected abstract ILog Logger { get; }

		/// <summary>Gets the Kafka Client Id of this instance</summary>
		protected abstract string GetClientId();

		/// <summary>Gets all connections.</summary>
		protected abstract IEnumerable<IAsyncKafkaConnection> GetAllConnections();

		/// <summary>Closes all connections</summary>
		protected void CloseAllConnections()
		{
			var connections = GetAllConnections();
			connections.ForEach(ch => ch.Disconnect());
		}

		/// <summary>
		/// Sends a metadata request to Kafka asking for metadata for all topics.
		/// The default implementation sends to any broker, retrying with all known brokers.
		/// <remarks>Override SendMetadataRequestAsync to change the default behavior.</remarks>
		/// </summary>
		/// <param name="cancellationToken"></param>
		protected Task<TopicMetadataResponse> SendMetadataRequestAllTopicsAsync(CancellationToken cancellationToken)
		{
			return SendMetadataRequestAsync(null, cancellationToken);
		}

		/// <summary>
		/// Sends a metadata request to Kafka asking for metadata for the specified topics.
		/// The default implementation sends to any broker, retrying with all known brokers.
		/// <remarks>Override SendMetadataRequestAsync to change the default behavior.</remarks>
		/// </summary>
		protected virtual Task<TopicMetadataResponse> SendMetadataRequestAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken)
		{
			var requestId = GetNextRequestId();
			var request = new TopicMetadataRequest(topics);
			return SendMetadataRequestAsync(request, requestId, cancellationToken);
		}

		/// <summary>
		/// Sends a metadata request to Kafka asking for metadata for the specified topics.
		/// The request is sent to any of the known brokers. If the first broker fails, then it retries with the next and so on.
		/// </summary>
		protected virtual Task<TopicMetadataResponse> SendMetadataRequestAsync(TopicMetadataRequest request, int requestId, CancellationToken cancellationToken)
		{
			return SendRequestToAnyBrokerAsync(request, TopicMetadataResponse.Deserialize, requestId, cancellationToken);
		}


		/// <summary> Gets the next request identifier. The identifier is guaranteed to be unique.</summary>
		public int GetNextRequestId()
		{
			return Interlocked.Increment(ref _requestId);
		}


		/// <summary>
		/// Sends the message to any broker. If the request to the first broker picked fails, the next broker is tried, and so on.
		/// </summary>
		/// <typeparam name="TResponse">The type of the response.</typeparam>
		/// <param name="request">The message.</param>
		/// <param name="deserializer">A delegate that deserializes the response to a instance of <typeparamref name="TResponse"/>.</param>
		/// <param name="requestId">The request identifier. A unique id is returned by </param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		/// <exception cref="KafkaException"></exception>
		protected async Task<TResponse> SendRequestToAnyBrokerAsync<TResponse>(IKafkaRequest request, ResponseDeserializer<TResponse> deserializer, int requestId, CancellationToken cancellationToken)
		{
			var connections = GetAllConnections();

			//Iterate over all connections, start with the ones that are already connected
			foreach(var connection in connections.OrderByDescending(c => c.IsConnected))
			{
				try
				{
					var response = await SendRequestAndReceiveAsync(connection, request, deserializer, requestId, cancellationToken);
					return response;
				}
				catch(Exception ex)
				{
					Logger.WarnException(ex, "Error while communicating with server {1} for request {0}. Trying next.", request, connection);
				}
			}
			var errorMessage = string.Format("Could not send request {0} to any server", request);
			Logger.ErrorFormat(errorMessage);
			throw new KafkaException(errorMessage);
		}

		protected virtual async Task<TResponse> SendRequestAndReceiveAsync<TResponse>(IAsyncKafkaConnection connection, IKafkaRequest request, ResponseDeserializer<TResponse> deserializer, int requestId, CancellationToken cancellationToken)
		{
			var message = new KafkaRequestWriteable(request, GetClientId());
			var responseBytes = await connection.RequestAsync(message, requestId, cancellationToken);
			var response = Deserialize(deserializer, responseBytes);
			return response;
		}

		protected async Task<ResponseResult<TResponse, TPayload>> SendRequestToLeaderAsync<TPayload, TResponse>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads, RequestBuilder<TPayload> requestBuilder, ResponseDeserializer<TResponse> responseDeserializer, bool allowTopicsToBeCreated, CancellationToken cancellationToken)
		{
			var payloadsByBroker = await GroupPayloadsByLeader(payloads, allowTopicsToBeCreated, cancellationToken);

			var failed = new List<TopicAndPartitionValue<TPayload>>();
			var tasks = new List<Tuple<IReadOnlyCollection<TopicAndPartitionValue<TPayload>>, Task<TResponse>,int, HostPort>>();
			foreach(var kvp in payloadsByBroker)
			{
				var broker = kvp.Key;
				var payloadsForBroker = kvp.Value;
				var requestId = GetNextRequestId();
				var message = requestBuilder(payloadsForBroker, requestId);
				var connection = GetConnectionForBroker(broker);
				var task = SendRequestAndReceiveAsync(connection, message, responseDeserializer, requestId, cancellationToken);
				tasks.Add(Tuple.Create(payloadsForBroker, task, requestId, connection.HostPort));
			}
			await Task.WhenAll(tasks.Select(t => t.Item2));
			var responses = new List<TResponse>();
			foreach(var tuple in tasks)
			{
				try
				{
					var response = tuple.Item2.Result;
					responses.Add(response);
				}
				catch(Exception ex)
				{
					failed.AddRange(tuple.Item1);
					var requestId = tuple.Item3;
					var hostPort = tuple.Item4;
					Logger.WarnException(ex, "Error while sending request {0} to server {1}", requestId, hostPort);
				}
			}
			return new ResponseResult<TResponse, TPayload>(responses, failed);
		}

		private async Task<Dictionary<Broker, IReadOnlyCollection<TopicAndPartitionValue<TPayload>>>> GroupPayloadsByLeader<TPayload>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads, bool allowTopicsToBeCreated, CancellationToken cancellationToken)
		{
			var tasks = payloads.Select(async payload =>
			{
				var leader = await GetLeaderAsync(payload.TopicAndPartition, allowTopicsToBeCreated, cancellationToken);

				return Tuple.Create(payload, leader);
			}).ToList();
			await Task.WhenAll(tasks.ToArray<Task>());

			var payloadsByBroker = tasks.GroupByToReadOnlyCollectionDictionary(task =>
			{
				var t = task.Result;
				var payload = t.Item1;
				var leader = t.Item2;
				if(leader == null) throw new LeaderNotAvailableException(payload.TopicAndPartition);
				return leader;
			}, task => task.Result.Item1);

			return payloadsByBroker;
		}


		protected abstract Task<Broker> GetLeaderAsync(TopicAndPartition topicAndPartition, bool allowTopicsToBeCreated, CancellationToken cancellationToken);


		private static TResponse Deserialize<TResponse>(ResponseDeserializer<TResponse> deserializer, byte[] response)
		{
			var readBuffer = new ReadBuffer(response);
			var deserialized = deserializer(readBuffer);
			return deserialized;
		}

		protected abstract IAsyncKafkaConnection GetConnectionForBroker(Broker broker);


	}
}