using System.IO;
using System.Net.Sockets;
using System.Threading;
using Kafka.Client.Api;

namespace Kafka.Client.IO
{
	public interface IKafkaMessageWriteable
	{
		int GetSize();
		void WriteTo(Stream stream, int correlationId);
		void WriteTo(Stream stream, int correlationId, CancellationToken cancellationToken);
		string GetNameForDebug();
		RequestApiKeys ApiKey { get; }
	}
}