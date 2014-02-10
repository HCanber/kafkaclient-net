using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
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
		private int _requestId = 0;


		/// <summary> Gets the logger for this instance.</summary>
		protected abstract ILog Logger { get; }

		/// <summary>Gets the Kafka Client Id of this instance</summary>
		protected abstract string GetClientId();

		/// <summary>Gets all connections.</summary>
		protected abstract IEnumerable<IKafkaConnection> GetAllConnections();

		/// <summary>Closes all connections</summary>
		protected void CloseAllConnections()
		{
			var connections = GetAllConnections();
			connections.ForEach(ch => ch.Disconnect());
		}

		/// <summary>
		/// Sends a metadata request to Kafka asking for metadata for all topics.
		/// The default implementation sends to any broker, retrying with all known brokers.
		/// <remarks>Override <see cref="SendMetadataRequest(TopicMetadataRequest,int)"/> to change the default behavior.</remarks>
		/// </summary>
		protected TopicMetadataResponse SendMetadataRequestAllTopics()
		{
			return SendMetadataRequest(null);
		}

		/// <summary>
		/// Sends a metadata request to Kafka asking for metadata for the specified topics.
		/// The default implementation sends to any broker, retrying with all known brokers.
		/// <remarks>Override <see cref="SendMetadataRequest(TopicMetadataRequest,int)"/> to change the default behavior.</remarks>
		/// </summary>
		protected virtual TopicMetadataResponse SendMetadataRequest(IReadOnlyCollection<string> topics)
		{
			var requestId = GetNextRequestId();
			var request = new TopicMetadataRequest(topics);
			var response = SendMetadataRequest(request, requestId);
			return response;
		}

		/// <summary>
		/// Sends a metadata request to Kafka asking for metadata for the specified topics.
		/// The request is sent to any of the known brokers. If the first broker fails, then it retries with the next and so on.
		/// </summary>
		protected virtual TopicMetadataResponse SendMetadataRequest(TopicMetadataRequest request, int requestId)
		{
			var response = SendRequestToAnyBroker(request, TopicMetadataResponse.Deserialize, requestId);
			return response;
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
		/// <returns></returns>
		/// <exception cref="KafkaException"></exception>
		protected TResponse SendRequestToAnyBroker<TResponse>(IKafkaRequest request, ResponseDeserializer<TResponse> deserializer, int requestId)
		{
			var connections = GetAllConnections();

			//Iterate over all connections, start with the ones that are already connected
			foreach(var connection in connections.OrderByDescending(c => c.IsConnected))
			{
				try
				{
					var response = SendRequestAndReceive(connection, request, deserializer, requestId);
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

		protected virtual TResponse SendRequestAndReceive<TResponse>(IKafkaConnection connection, IKafkaRequest request, ResponseDeserializer<TResponse> deserializer, int requestId)
		{
			lock(connection)
			{
				var message=new KafkaRequestWriteable(request,GetClientId());
				var responseBytes = connection.Request(message, requestId);
				var response = Deserialize(deserializer, responseBytes);
				return response;
			}
		}

		protected List<TResponse> SendRequestToLeader<TPayload, TResponse>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads, RequestBuilder<TPayload> requestBuilder, ResponseDeserializer<TResponse> responseDeserializer, out IReadOnlyCollection<TopicAndPartitionValue<TPayload>> failedItems)
		{
			var payloadsByBroker = GroupPayloadsByBroker<TPayload>(payloads);

			var failed = new List<TopicAndPartitionValue<TPayload>>();
			var responses = new List<TResponse>();
			foreach(var kvp in payloadsByBroker)
			{
				var broker = kvp.Key;
				var payloadsForBroker = kvp.Value;
				var requestId = GetNextRequestId();
				var message = requestBuilder(payloadsForBroker,requestId);
				var connection = GetConnectionForBroker(broker);
				try
				{
					var response = SendRequestAndReceive(connection, message, responseDeserializer, requestId);
					responses.Add(response);
				}
				catch(Exception ex)
				{
					failed.AddRange(payloadsForBroker);
					Logger.WarnException(ex, "Error while sending request {0} to server {1}", message, connection);
				}
			}
			failedItems = failed;
			return responses;
		}

		private Dictionary<Broker, IReadOnlyCollection<TopicAndPartitionValue<TPayload>>> GroupPayloadsByBroker<TPayload>(IEnumerable<TopicAndPartitionValue<TPayload>> payloads)
		{
			var payloadsByBroker = payloads.GroupByToReadOnlyCollectionDictionary(payload =>
			{
				var topicAndPartition = payload.TopicAndPartition;
				var leader = GetLeader(topicAndPartition);
				if(leader == null) throw new PartitionUnavailableException(topicAndPartition, "No leader for " + topicAndPartition);
				return leader;
			});
			
			return payloadsByBroker;
		}


		protected abstract Broker GetLeader(TopicAndPartition topicAndPartition);


		private static TResponse Deserialize<TResponse>(ResponseDeserializer<TResponse> deserializer, byte[] response)
		{
			var readBuffer = new ReadBuffer(response);
			var deserialized = deserializer(readBuffer);
			return deserialized;
		}

		protected abstract IKafkaConnection GetConnectionForBroker(Broker broker);
	}
}