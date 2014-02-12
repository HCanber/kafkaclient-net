namespace Kafka.Client.Api
{
	public enum KafkaError : short
	{
		Unknown = -1,
		NoError = 0,
		OffsetOutOfRange = 1,
		InvalidMessage = 2,
		UnknownTopicOrPartition = 3,
		InvalidFetchSize = 4,
		LeaderNotAvailable = 5,
		NotLeaderForPartition = 6,
		RequestTimedOut = 7,
		BrokerNotAvailable = 8,
		ReplicaNotAvailable = 9,
		MessageSizeTooLarge = 10,
		StaleControllerEpoch = 11,
		OffsetMetadataTooLarge = 12,
		StaleLeaderEpoch = 13,
	}
}