namespace Kafka.Client.Api
{
	public enum KafkaError : short
	{
		UnknownCode = -1,
		NoError = 0,
		OffsetOutOfRangeCode = 1,
		InvalidMessageCode = 2,
		UnknownTopicOrPartitionCode = 3,
		InvalidFetchSizeCode = 4,
		LeaderNotAvailableCode = 5,
		NotLeaderForPartitionCode = 6,
		RequestTimedOutCode = 7,
		BrokerNotAvailableCode = 8,
		ReplicaNotAvailableCode = 9,
		MessageSizeTooLargeCode = 10,
		StaleControllerEpochCode = 11,
		OffsetMetadataTooLargeCode = 12,
		StaleLeaderEpochCode = 13,
	}
}