using KafkaClient.IO;
using KafkaClient.Utils;

namespace KafkaClient.Api
{
	public abstract class RequestBase : RequestOrResponse
	{
		private readonly short _apiKey;
		private readonly int _correlationId;
		private readonly string _clientId;

		protected RequestBase(short apiKey, int correlationId, string clientId)
		{
			_apiKey = apiKey;
			_correlationId = correlationId;
			_clientId = clientId;
		}

		protected override int SizeInBytes
		{
			get
			{
				const int shortSize = BitConversion.ShortSize;
				const int intSize = BitConversion.IntSize;
				return
					shortSize + // ApiKey
					shortSize + // Version
					intSize +   // Correlation Id
					KafkaWriter.GetShortStringLength(_clientId)+
					MessageSizeInBytes;
			}
		}

		protected abstract int MessageSizeInBytes { get; }

		protected virtual short Version { get { return 0; } }

		protected override void WriteTo(KafkaWriter writer)
		{
			writer.WriteShort(_apiKey);
			writer.WriteShort(Version);
			writer.WriteInt(_correlationId);
			writer.WriteShortString(_clientId);
			WriteRequestMessage(writer);
		}

		protected abstract void WriteRequestMessage(KafkaWriter writer);

		//protected abstract void HandleError(Exception e, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {}

		/* The purpose of this API is to return a string description of the Request mainly for the purpose of request logging.
		*  This API has no meaning for a Response object.
		* @param details If this is false, omit the parts of the request description that are proportional to the number of
		*                topics or partitions. This is mainly to control the amount of request logging. */
		//def describe(details: Boolean):String

	}
}