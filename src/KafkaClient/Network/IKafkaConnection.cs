using Kafka.Client.IO;

namespace Kafka.Client.Network
{
	public interface IKafkaConnection
	{
		void Disconnect();
		byte[] Request(IKafkaMessageWriteable msg, int requestId);
		HostPort HostPort { get; }
		bool IsConnected { get; }
	}
}