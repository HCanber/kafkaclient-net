using System;
using System.IO;
using Kafka.Client.IO;

namespace Kafka.Client.Api
{
	public static class KafkaRequestExtensions
	{
		public static byte[] Serialize(this IKafkaRequest request, string clientId, int correlationId)
		{
			var buffer = new Byte[request.GetSize(clientId)];
			var stream = new MemoryStream(buffer);
			request.WriteTo(stream, clientId,correlationId);
			return buffer;
		}
	}
}