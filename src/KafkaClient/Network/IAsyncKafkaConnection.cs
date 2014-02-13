using System.Threading;
using System.Threading.Tasks;
using Kafka.Client.IO;
using Kafka.Client.JetBrainsAnnotations;

namespace Kafka.Client.Network
{
	public interface IAsyncKafkaConnection
	{
		HostPort HostPort { get; }
		bool IsConnected { get; }
		void Connect();
		Task ConnectAsync();
		Task<byte[]> RequestAsync([NotNull] IKafkaMessageWriteable message, int requestId);
		Task<byte[]> RequestAsync([NotNull] IKafkaMessageWriteable message, int requestId, CancellationToken cancellationToken);
		void Disconnect();
	}
}