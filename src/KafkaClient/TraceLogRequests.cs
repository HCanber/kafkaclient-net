using System;

namespace Kafka.Client
{
	[Flags]
	public enum TraceLogRequests
	{
		None = 0,
		Produce = 1,
		Fetch = 2,
		Offsets = 4,
		Metadata = 8,
		LeaderAndIsr = 16,
		StopReplica = 32,
		UpdateMetadata = 64,
		ControlledShutdown = 128,
		OffsetCommit = 256,
		OffsetFetch = 512,
		AllButFetchAndMeta = Produce | Offsets | LeaderAndIsr | StopReplica | UpdateMetadata | ControlledShutdown | OffsetCommit | OffsetFetch,
		AllButFetch = AllButFetchAndMeta | Metadata,
		All = AllButFetch | Fetch,
	}
}