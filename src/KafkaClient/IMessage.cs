using System;

namespace Kafka.Client
{
	public interface IMessage
	{
		bool HasKey { get; }
		int KeySize { get; }
		ArraySegment<byte> Key { get; }
		int ValueSize { get; }
		ArraySegment<byte> Value { get; }
		byte Magic { get; }
		byte Attributes { get; }
		uint Checksum { get; }
		bool IsValid { get; }
		uint ComputeChecksum();
	}
}