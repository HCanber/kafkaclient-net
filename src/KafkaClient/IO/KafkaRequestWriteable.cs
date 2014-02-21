using System.IO;
using System.Threading;
using Kafka.Client.Api;

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
			WriteTo(stream,correlationId,CancellationToken.None);
		}

		public void WriteTo(Stream stream, int correlationId, CancellationToken cancellationToken)
		{
			_request.WriteTo(stream,_clientId,correlationId,cancellationToken);
		}

		public override string ToString()
		{
			return GetNameForDebug();
		}

		public string GetNameForDebug()
		{
			return _request.GetNameForDebug();			
		}

		public RequestApiKeys ApiKey { get { return _request.ApiKey; } }
	}
}