using System;
using System.IO;
using KafkaClient.IO;
using KafkaClient.Utils;

namespace KafkaClient.Api
{
	public abstract class RequestOrResponse
	{
		protected abstract int SizeInBytes { get; }

		/// <summary>
		/// Writes the request or response to an array and returns it.
		/// </summary>
		public byte[] Write()
		{
			var sizeInBytes = SizeInBytes;
			var totalSize = sizeInBytes + BitConversion.IntSize;
			var buffer = new Byte[totalSize];
			var writer = new KafkaBinaryWriter(new MemoryStream(buffer));
			writer.WriteInt(sizeInBytes);
			WriteTo(writer);
			return buffer;
		}

		protected abstract void WriteTo(KafkaWriter writer);
	}
}