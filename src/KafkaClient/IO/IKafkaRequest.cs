using System.IO;

namespace Kafka.Client.IO
{
	public interface IKafkaRequest
	{
		int GetSize(string clientId);
		void WriteTo(Stream stream, string clientId, int correlationId);
	}
}