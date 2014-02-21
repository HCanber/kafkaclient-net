using System.IO;
using System.Threading;

namespace Kafka.Client.IO
{
	public interface IKafkaRequest
	{
		int GetSize(string clientId);
		void WriteTo(Stream stream, string clientId, int correlationId);
		void WriteTo(Stream stream, string clientId, int correlationId, CancellationToken cancellationToken);
		string GetNameForDebug();
	}
}