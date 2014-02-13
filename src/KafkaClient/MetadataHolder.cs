using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kafka.Client.Api;
using Kafka.Client.Exceptions;
using Kafka.Client.JetBrainsAnnotations;
using Kafka.Client.Utils;

namespace Kafka.Client
{
	public class MetadataHolder
	{
		private readonly KafkaClient _client;
		private readonly ConcurrentDictionary<string, TopicMetadata> _metadataByTopic = new ConcurrentDictionary<string, TopicMetadata>();


		public MetadataHolder([NotNull] KafkaClient client)
		{
			if(client == null) throw new ArgumentNullException("client");
			_client = client;
		}

		public void ResetAllMetadata()
		{
			_metadataByTopic.Clear();
		}

		public void ResetMetadataForTopic(string topic)
		{
			TopicMetadata partitions;
			_metadataByTopic.TryRemove(topic, out partitions);
		}

		public async Task<TopicMetadata> GetMetadataForTopic([NotNull] string topic, CancellationToken cancellationToken, bool useCachedValues = true)
		{
			if(topic == null) throw new ArgumentNullException("topic");
			if(topic.Length == 0) throw new ArgumentException("topic");
			var metas = await GetMetaForTopicsAsync(new[] {topic}, useCachedValues ? Fetch.OnlyExistingTopicsAllowServer | Fetch.Force : Fetch.OnlyExistingTopicsAllowServer, cancellationToken);
			var meta =metas[0];
			switch(meta.Error)
			{
				case KafkaError.UnknownTopicOrPartition:
					throw new UnknownTopicException(topic);
				case KafkaError.LeaderNotAvailable:
					//This error code is sent as an error on the topic level when the topic did not exist, but config has autoCreateTopicsEnable=true.
					//So the topic is created but no leader is known at this time. See Kafka source code: handleTopicMetadataRequest() in KafkaApis.scala
					throw new TopicCreatedNoLeaderYetException(new TopicAndPartition(topic, -1));
			}
			return meta;
		}

		public Task<IReadOnlyList<TopicMetadata>> GetRawMetadataForTopicsAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken, bool useCachedValues = true)
		{
			var metas = GetMetaForTopicsAsync(topics, useCachedValues ? Fetch.OnlyExistingTopicsAllowServer | Fetch.Force : Fetch.OnlyExistingTopicsAllowServer, cancellationToken);
			return metas;
		}

		public async Task<IReadOnlyList<TopicItem<IReadOnlyList<int>>>> GetPartitionsForTopicsAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken)
		{
			var metaForTopics = await GetMetaForTopicsAsync(topics, Fetch.OnlyExistingTopicsAllowServer, cancellationToken);
			var result = new List<TopicItem<IReadOnlyList<int>>>();
			foreach(var topicMetadata in metaForTopics)
			{
				var partitions = topicMetadata.Error == KafkaError.UnknownTopicOrPartition
					? null
					: topicMetadata.PartionMetaDatas.Keys.ToImmutableList();
				result.Add(TopicItem.Create(topicMetadata.Topic, partitions));
			}
			return result;
		}


		/// <summary>
		/// Gets the leader for the specified topic and partition.
		/// Returns <c>null</c> if no leader exists.
		/// </summary>
		/// <returns>The leader. <c>null</c> if no leader exists.</returns>
		/// <exception cref="UnknownTopicException">Thrown if topic do not exist</exception>
		/// <exception cref="KafkaInvalidPartitionException">Thrown if partition do not exist</exception>
		public async Task<Broker> GetLeaderAsync(TopicAndPartition topicAndPartition, CancellationToken cancellationToken, bool allowTopicsToBeCreated = false)
		{
			var topic = topicAndPartition.Topic;
			var metas = await GetMetaForTopicsAsync(new[] { topic }, allowTopicsToBeCreated ? Fetch.AllowFetchFromServer : Fetch.OnlyExistingTopicsAllowServer, cancellationToken);
			var meta = metas[0];
			switch(meta.Error)
			{
				case KafkaError.UnknownTopicOrPartition:
					throw new UnknownTopicException(topic);
				case KafkaError.LeaderNotAvailable:
					//This error code is sent as an error on the topic level when the topic did not exist, but config has autoCreateTopicsEnable=true.
					//So the topic is created but no leader is known at this time. See Kafka source code: handleTopicMetadataRequest() in KafkaApis.scala
					throw new TopicCreatedNoLeaderYetException(topicAndPartition);
			}
			PartitionMetadata partitionMetadata;
			if(!meta.PartionMetaDatas.TryGetValue(topicAndPartition.Partition, out partitionMetadata))
			{
				throw new UnknownPartitionException(topicAndPartition);
			}
			if(partitionMetadata.Error == KafkaError.LeaderNotAvailable) return null;
			//In this stage we can safely ignore other errors, such as Replica Not Available or 
			//In Sync Replica Not Available. Those errors do not affect whose the leader.
			return partitionMetadata.Leader;
		}

		private async Task<IReadOnlyList<TopicMetadata>> GetMetaForTopicsAsync(IReadOnlyCollection<string> topics, Fetch fetch, CancellationToken cancellationToken, int retries = 1)
		{
			var onlyExistingTopics = fetch.HasFlag(Fetch.OnlyExistingTopics);
			var allowFetch = fetch.HasFlag(Fetch.AllowFetchFromServer);

			//First handle the case when we want all topics
			var shouldGetAllTopics = topics == null;
			if(shouldGetAllTopics)
			{
				if(allowFetch)
				{
					//Always fetch the latest from the server if we're allowed
					return await GetAndCacheMetadataForTopicsAsync(null, cancellationToken);
				}
				return _metadataByTopic.Values.ToImmutableList();
			}


			//Should we force a refresh of the cache?
			var shouldRefreshFirst = false;
			if(fetch.HasFlag(Fetch.Force))
			{
				shouldRefreshFirst = true;
			}
			//If we're only allowed to use existing topics, and there are some topics that do not exist in the cache, we want to refresh
			else if(onlyExistingTopics && topics.Any(topic => !_metadataByTopic.ContainsKey(topic)))
			{
				shouldRefreshFirst = true;
			}
			if(shouldRefreshFirst && allowFetch)
			{
				await GetAndCacheMetadataForTopicsAsync(onlyExistingTopics ? null : topics, cancellationToken);
				retries = 0;	//No need to retry since we already have updated the cache with the latest
			}

			while(true)
			{
				var existing = new List<TopicMetadata>();
				var missing = new List<string>();
				var errors = new List<TopicMetadata>();
				var keepOnesWithErrors = retries == 0;
				foreach(var topic in topics)
				{
					TopicMetadata metadata;
					if(!_metadataByTopic.TryGetValue(topic, out metadata))
						missing.Add(topic);
					else
					{
						if(keepOnesWithErrors || metadata.Error == KafkaError.NoError)
							existing.Add(metadata);
						else
							errors.Add(metadata); //previously stored result contained errors.
					}
				}
				var metadataExistsForAllRequestedTopics = missing.Count == 0 && errors.Count == 0;
				if(metadataExistsForAllRequestedTopics) return existing;

				var shouldRetry = allowFetch && retries > 0;
				if(shouldRetry)
				{
					await GetAndCacheMetadataForTopicsAsync(onlyExistingTopics ? null : missing.Concat(errors.Select(m => m.Topic)).ToList(), cancellationToken);
					retries = retries - 1;
				}
				else
				{
					return existing
						.Concat(errors)
						.Concat(missing.Select(topic => new TopicMetadata(topic, EmptyReadOnly<PartitionMetadata>.Instance, KafkaError.UnknownTopicOrPartition)))
						.ToList();
				}
			}
		}

		private async Task<IReadOnlyList<TopicMetadata>> GetAndCacheMetadataForTopicsAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken)
		{
			var metadatas = await LoadMetaForTopicsAsync(topics, cancellationToken);
			foreach(var metadata in metadatas)
			{
				//Only store metadata for topics that do exist. Otherwise the dictionary could be filled 
				//with "infinite" amount of non existing topics.
				if(metadata.Error != KafkaError.UnknownTopicOrPartition)
				{
					_metadataByTopic[metadata.Topic] = metadata;
				}
			}
			return metadatas;
		}

		private async Task<IReadOnlyList<TopicMetadata>> LoadMetaForTopicsAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken)
		{
			var response =await _client.SendMetadataRequestForTopicsAsync(topics ?? new string[0], cancellationToken);

			return response.TopicMetadatas;
		}

		[Flags]
		private enum Fetch
		{
			AllowFetchFromServer = 1,
			Force = 2 + AllowFetchFromServer,
			OnlyExistingTopics = 4,
			OnlyExistingTopicsAllowServer = AllowFetchFromServer + OnlyExistingTopics,
		}
	}
}