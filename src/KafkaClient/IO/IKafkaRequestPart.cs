using System.IO;

namespace Kafka.Client.IO
{
	public interface IKafkaRequestPart
	{
		int GetSize();
		void WriteTo(KafkaWriter writer);
		
	}
}