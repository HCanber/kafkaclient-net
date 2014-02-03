namespace Kafka.Client.Api
{
	public static class Request
	{
		public const int OrdinaryConsumerId = -1;
		public const int DebuggingConsumerId = -2;

		// Followers use broker id as the replica id, which are non-negative int.
		public static bool IsReplicaIdFromFollower(int replicaId)
		{
			return replicaId >= 0;
		}
	}
}