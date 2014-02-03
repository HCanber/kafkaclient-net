using System.IO;

namespace Kafka.Client.IO
{
	public class KafkaRequestWriteable : IKafkaMessageWriteable
	{
		private readonly IKafkaRequest _request;
		private readonly string _clientId;

		public KafkaRequestWriteable(IKafkaRequest request, string clientId)
		{
			_request = request;
			_clientId = clientId;
		}

		public int GetSize()
		{
			return _request.GetSize(_clientId);
		}

		public void WriteTo(Stream stream, int correlationId)
		{
			_request.WriteTo(stream,_clientId,correlationId);
		}
	}
}