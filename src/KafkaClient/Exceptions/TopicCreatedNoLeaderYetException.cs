using System;
using Kafka.Client.Api;

namespace Kafka.Client.Exceptions
{
	/// <summary>
	/// This exception is thrown when trying to store a value in Kafka, i.e. sending a ProduceRequest,
	/// but the topic did not exist. Config has autoCreateTopicsEnable=true so the topic has been
	/// created but no leader is known at this time. Try resending the message.
	/// </summary>
	[Serializable]
	public class TopicCreatedNoLeaderYetException : LeaderNotAvailableException, IRetryableError
	{
		public TopicCreatedNoLeaderYetException(TopicAndPartition topicAndPartition) 
			: base(new[]{topicAndPartition}, "The topic was created but no leader exists for {0}. Try again in a while.")
		{
		}
	}
}