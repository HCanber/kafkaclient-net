using System.Diagnostics;
using System.IO;
using Kafka.Client.IO;
using Kafka.Client.Utils;

namespace Kafka.Client.Api
{
	public abstract class RequestBase : IKafkaRequest
	{
		private readonly short _apiKey;

		protected RequestBase(short apiKey)
		{
			_apiKey = apiKey;
		}

		protected abstract int MessageSizeInBytes { get; }

		protected virtual short Version { get { return 0; } }

		public virtual int GetSize(string clientId)
		{
				const int shortSize = BitConversion.ShortSize;
				const int intSize = BitConversion.IntSize;
				return
					shortSize + // Api Key
					shortSize + // Api Version
					intSize +   // Correlation Id
					KafkaWriter.GetShortStringLength(clientId)+
					MessageSizeInBytes;
			
		}

		protected virtual void WriteTo(KafkaWriter writer, string clientId, int correlationId)
		{
			writer.WriteShort(_apiKey);
			writer.WriteShort(Version);
			writer.WriteInt(correlationId);
			writer.WriteShortString(clientId);
			WriteRequestMessage(writer);
		}

		protected abstract void WriteRequestMessage(KafkaWriter writer);

		public void WriteTo(Stream stream, string clientId, int correlationId)
		{
			var writer = new KafkaBinaryWriter(stream);
			WriteTo(writer,clientId,correlationId);
			Debug.Assert(writer.NumberOfWrittenBytes == GetSize(clientId));
		}
	}
}