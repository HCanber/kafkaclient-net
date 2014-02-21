using System;
using System.IO;
using Kafka.Client.JetBrainsAnnotations;

namespace Kafka.Client.IO
{
	public class DualWriteableStream : Stream
	{
		private readonly Stream _stream1;
		private readonly Stream _stream2;

		public DualWriteableStream([NotNull] Stream stream1, [NotNull] Stream stream2)
		{
			if(stream1 == null) throw new ArgumentNullException("stream1");
			if(stream2 == null) throw new ArgumentNullException("stream2");
			_stream1 = stream1;
			_stream2 = stream2;
		}

		public override void Flush()
		{
			_stream1.Flush();
			_stream2.Flush();
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotSupportedException();
		}

		public override void SetLength(long value)
		{
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			throw new NotSupportedException();
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			_stream1.Write(buffer, offset, count);
			_stream2.Write(buffer, offset, count);
		}

		public override bool CanRead { get { return false; } }

		public override bool CanSeek { get { return false; } }

		public override bool CanWrite { get { return _stream1.CanWrite && _stream2.CanWrite; } }

		public override long Length { get { throw new NotSupportedException(); } }

		public override long Position { get { throw new NotSupportedException(); } set { throw new NotSupportedException(); } }
	}
}