using System.Collections.Generic;
using System.Linq;
using KafkaClient.IO;

namespace KafkaClient.Api
{
	public class PartitionMetadata
	{
		private readonly int _partitionId;
		private readonly Broker _leader;
		private readonly IReadOnlyList<Broker> _replicas;
		private readonly IReadOnlyList<Broker> _inSyncReplicas;
		private readonly short _errorCode;

		private PartitionMetadata(int partitionId, Broker leader, IReadOnlyList<Broker> replicas, IReadOnlyList<Broker> inSyncReplicas, short errorCode)
		{
			_partitionId = partitionId;
			_leader = leader;
			_replicas = replicas;
			_inSyncReplicas = inSyncReplicas;
			_errorCode = errorCode;
		}

		public int PartitionId { get { return _partitionId; } }

		public Broker Leader { get { return _leader; } }

		public IReadOnlyList<Broker> Replicas { get { return _replicas; } }

		public IReadOnlyList<Broker> InSyncReplicas { get { return _inSyncReplicas; } }

		public short ErrorCode { get { return _errorCode; } }



		public static PartitionMetadata Deserialize(IReadBuffer buffer, IReadOnlyDictionary<int, Broker> brokersById)
		{
			var errorCode = buffer.ReadShortInRange(-1, short.MaxValue, "Error code");
			var partitionId = buffer.ReadIntInRange(-1, short.MaxValue, "Partition Id");
			var leaderId = buffer.ReadInt();
			var leader = brokersById[leaderId];

			var replicaIds = buffer.ReadArray(buf => buf.ReadInt());
			var replicas = replicaIds.Select(replica => brokersById[replica]).ToList();

			var inSyncReplicaIds = buffer.ReadArray(buf => buf.ReadInt());
			var inSyncReplicas = inSyncReplicaIds.Select(replica => brokersById[replica]).ToList();

			return new PartitionMetadata(partitionId, leader, replicas, inSyncReplicas, errorCode);
		}

	}
}