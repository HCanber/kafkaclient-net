using System.IO;
using System.Threading.Tasks;
using KafkaClient.Api;

namespace KafkaClient.IO
{
	public class WriteBuffer
	{
		private readonly byte[] _bytes;

		private WriteBuffer(byte[] bytes)
		{
			_bytes = bytes;
		}

		public int Size { get { return _bytes.Length; } }

		public static WriteBuffer Create(RequestOrResponse response)
		{
			var bytes = response.Write();
			return new WriteBuffer(bytes);
		}

		public void WriteTo(Stream stream)
		{
			stream.Write(_bytes, 0, _bytes.Length);
		}

		public Task WriteToAsync(Stream stream)
		{
			return stream.WriteAsync(_bytes, 0, _bytes.Length);
		}
	}
}