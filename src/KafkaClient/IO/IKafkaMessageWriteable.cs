using System.IO;
using System.Threading;

namespace Kafka.Client.IO
{
	public interface IKafkaMessageWriteable
	{
		int GetSize();
		void WriteTo(Stream stream, int correlationId);
		void WriteTo(Stream stream, int correlationId, CancellationToken cancellationToken);
	}
}