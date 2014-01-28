using System;

namespace KafkaClient.IO
{
	public static class EmptyArraySegment<T>
	{
		public static ArraySegment<T> Instance = new ArraySegment<T>(new T[0]);
	}
}