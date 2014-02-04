using System;
using System.IO;

namespace Kafka.Client.IO
{
	public interface IRandomAccessReadBuffer
	{
		byte ReadByte(int position);
		short ReadShort(int position);
		int ReadInt(int position);
		uint ReadUInt(int position);
		long ReadLong(int position);
		string ReadShortString(int position);
		//IReadOnlyList<T> ReadArray<T>(int position,Func<IReadBuffer, T> parseItem);
		IRandomAccessReadBuffer Slice(int position, int size);
		ArraySegment<byte> ReadByteArraySegment(int position, int size);
		int Count { get; }
		ArraySegment<byte> GetAsArraySegment();
		void WriteTo(Stream stream);
	}
}