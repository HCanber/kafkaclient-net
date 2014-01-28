using System;
using System.Collections.Generic;

namespace KafkaClient.IO
{
	public interface IReadBuffer
	{
		byte ReadByte();
		short ReadShort();
		int ReadInt();
		long ReadLong();
		string ReadShortString();
		IReadOnlyList<T> ReadArray<T>(Func<IReadBuffer, T> parseItem);
		IReadBuffer Slice(int size);
		ArraySegment<byte> ReadByteArraySegment(int size);
		IRandomAccessReadBuffer GetRandomAccessReadBuffer(int size);
		int Count { get; }
		int CurrentPosition { get; }
		int BytesLeft { get; }
	}
}