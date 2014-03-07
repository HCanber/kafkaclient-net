using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;

namespace Kafka.Client
{
	public class SimpleProducer : ProducerBase
	{
		private readonly RequiredAck _requiredAcks;
		private readonly int _ackTimeoutMs;

		public SimpleProducer(IKafkaClient client, RequiredAck requiredAcks = RequiredAck.WrittenToDiskByLeader, int ackTimeoutMs = 1000)
			: base(client)
		{
			_requiredAcks = requiredAcks;
			_ackTimeoutMs = ackTimeoutMs;
		}

		/// <summary>
		/// Sends a message to the leader responsible for the topic-partition and returns the offset. If anything fails, then up to 
		/// <paramref name="attemptsBeforeFailing"/> attempts are made. If it still fails, a <see cref="ProduceFailedException"/> is thrown.
		/// This contains offsets for the ones that succeeded and error descriptions for the ones that failed.
		/// </summary>
		/// <param name="topic">The topic</param>
		/// <param name="partition">The partition</param>
		/// <param name="key">The key. Set it to null if you don't have a key</param>
		/// <param name="value">The value</param>
		/// <param name="attemptsBeforeFailing">The number of attempts before failing.</param>
		/// <returns>A task that eventually will contain the offsets</returns>
		/// <exception cref="ProduceFailedException">Thrown when all attempts failed.</exception>
		public async Task<TopicAndPartitionValue<long>> SendAsync(string topic, int partition, byte[] key, byte[] value, int attemptsBeforeFailing = 3)
		{
			return await SendAsync(topic, partition, key, value, CancellationToken.None, attemptsBeforeFailing);
		}

		/// <summary>
		/// Sends a message to the leader responsible for the topic-partition and returns the offset. If anything fails, then up to 
		/// <paramref name="attemptsBeforeFailing"/> attempts are made. If it still fails, a <see cref="ProduceFailedException"/> is thrown.
		/// This contains offsets for the ones that succeeded and error descriptions for the ones that failed.
		/// </summary>
		/// <param name="topic">The topic</param>
		/// <param name="partition">The partition</param>
		/// <param name="key">The key. Set it to null if you don't have a key</param>
		/// <param name="value">The value</param>
		/// <param name="cancellationToken">The cancellation token, that can be used to cancel the operation.</param>
		/// <param name="attemptsBeforeFailing">The number of attempts before failing.</param>
		/// <returns>A task that eventually will contain the offsets</returns>
		/// <exception cref="ProduceFailedException">Thrown when all attempts failed.</exception>
		public async Task<TopicAndPartitionValue<long>> SendAsync(string topic, int partition, byte[] key, byte[] value, CancellationToken cancellationToken, int attemptsBeforeFailing = 3)
		{
			var topicAndPartition = new TopicAndPartition(topic, partition);

			var offsets = await SendAsync(new[] { TopicAndPartitionValue.Create(topicAndPartition, (IEnumerable<IMessage>)new[] { new Message(key, value) }) }, cancellationToken, attemptsBeforeFailing);
			return offsets[0];
		}

		/// <summary>
		/// Sends messages to the leaders responsible for each topic and partition and returns the offsets. If anything fails, then up to 
		/// <paramref name="attemptsBeforeFailing"/> attempts are made. If it still fails, a <see cref="ProduceFailedException"/> is thrown.
		/// This contains offsets for the ones that succeeded and error descriptions for the ones that failed.
		/// </summary>
		/// <param name="messages">The messages, grouped by topic-partition.</param>
		/// <param name="attemptsBeforeFailing">The number of attempts before failing.</param>
		/// <returns>A task that eventually will contain the offsets</returns>
		/// <exception cref="ProduceFailedException">Thrown when all attempts failed.</exception>
		public async Task<IReadOnlyList<TopicAndPartitionValue<long>>> SendAsync(IEnumerable<TopicAndPartitionValue<IEnumerable<IMessage>>> messages, int attemptsBeforeFailing = 3)
		{
			return await SendAsync(messages, CancellationToken.None, attemptsBeforeFailing);
		}

		/// <summary>
		/// Sends messages to the leaders responsible for each topic and partition and returns the offsets. If anything fails, then up to 
		/// <paramref name="attemptsBeforeFailing"/> attempts are made. If it still fails, a <see cref="ProduceFailedException"/> is thrown.
		/// This contains offsets for the ones that succeeded and error descriptions for the ones that failed.
		/// </summary>
		/// <param name="messages">The messages, grouped by topic-partition.</param>
		/// <param name="cancellationToken">The cancellation token, that can be used to cancel the operation.</param>
		/// <param name="attemptsBeforeFailing">The number of attempts before failing.</param>
		/// <returns>A task that eventually will contain the offsets</returns>
		/// <exception cref="ProduceFailedException">Thrown when all attempts failed.</exception>
		public async Task<IReadOnlyList<TopicAndPartitionValue<long>>> SendAsync(IEnumerable<TopicAndPartitionValue<IEnumerable<IMessage>>> messages, CancellationToken cancellationToken, int attemptsBeforeFailing = 3)
		{
			var messagesToSend = messages.ToList();
			var attempt = 1;
			var offsets = new List<TopicAndPartitionValue<long>>();
					var errors = new List<TopicAndPartitionValue<KafkaError>>();
					var metaDataErrors = new List<TopicAndPartitionValue<KafkaError>>();
			while(true)
			{
				cancellationToken.ThrowIfCancellationRequested();
				try
				{
					ResponseResult<ProduceResponse, IEnumerable<IMessage>> result = await SendProduceAsync(messagesToSend, _requiredAcks, _ackTimeoutMs, cancellationToken);
					var failedMessages = result.FailedItems;
					errors.Clear();
					metaDataErrors.Clear();
					foreach(var response in result.Responses)
					{						
						foreach(var kvp in response.StatusesByTopic)
						{
							//var topic = kvp.Key;
							var producerResponseStatuses = kvp.Value;
							foreach(var status in producerResponseStatuses)
							{
								var error = status.Error;
								if(error != KafkaError.NoError)
								{
									offsets.Add(TopicAndPartitionValue.Create(status.TopicAndPartition,status.Offset));
								}
								else
								{
									var errorTuple = TopicAndPartitionValue.Create(status.TopicAndPartition,error);
									if(IsMetadataError(error))
										metaDataErrors.Add(errorTuple);
									errors.Add(errorTuple);
								}
							}
						}
					}

					//If we have no failed items (unexpected errors) and no "expected" errors then return all offsets
					var hasErrors = errors.Count>0;
					if(failedMessages.Count == 0 && !hasErrors)
					{
						return offsets;
					}
					//Lookup which messages failed (if any) and put them into a list
					var messagesWithErrors = new List<FailedMessages<KafkaError>>();
					if(errors.Count > 0)
					{
						var messagesByTopicAndPartition = messagesToSend.ToDictionary(m=>m.TopicAndPartition);
						foreach(var error	 in errors)
						{
							var message = messagesByTopicAndPartition[error.TopicAndPartition];
							messagesWithErrors.Add(FailedMessages.Create(message, error.Value));
						}
					}
					//If we have made all attempts we're allowed to do, throw an exception containing the failed messages, and the offsets we have so far
					if(attempt > attemptsBeforeFailing)
					{
						throw new ProduceFailedException(failedMessages.Select(t => FailedMessages.Create(t.Item1, t.Item2)).ToList(), messagesWithErrors, offsets);
					}
					//ok, so we have more attempts, let's see if we have any errors related to metadata. If we do reset meta data for those topics, before we retry
					if(metaDataErrors.Count > 0)
					{
						foreach(var topic in metaDataErrors.Select(m=>m.TopicAndPartition.Topic).Distinct())
						{
							Client.ResetMetadataForTopic(topic);
						}
					}

					//Create the new list of messages to send, i.e. the ones which totally failed and the ones for which the broker returned error
					messagesToSend.Clear();
					messagesToSend.AddRange(failedMessages.Select(f => f.Item1));
					messagesToSend.AddRange(messagesWithErrors.Select(m=>m.Messages));
				}
				catch(Exception e)
				{
					if(e is IRetryableError)
					{
						//An example when we get RetryableError exceptions:
						//   When a topic did not exist, but config has autoCreateTopicsEnable=true.
						//   The topic has been created but no leader is known at this time. Sleep just a little bit to wait for Kafka to elect a leader.
						Thread.Sleep(attempt * 200);
					}
					else
						throw;
				}				
				attempt++;
			}
		}

		private static bool IsMetadataError(KafkaError error)
		{
			return error==KafkaError.NotLeaderForPartition;
		}

		public void Close()
		{
			Client.Close();
		}
	}
}