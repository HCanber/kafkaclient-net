using System.IO;

namespace Kafka.Client.IO
{
	public interface IKafkaMessageWriteable
	{
		int GetSize();
		void WriteTo(Stream stream, int correlationId);
	}
}