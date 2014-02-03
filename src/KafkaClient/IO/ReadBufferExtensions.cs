using Kafka.Client.Exceptions;

namespace Kafka.Client.IO
{
	public static class ReadBufferExtensions
	{
		public static short ReadShortInRange(this IReadBuffer buffer, short min, short max, string name)
		{
			var value = buffer.ReadShort();
			if(value < min || value > max)
				throw new KafkaException(string.Format("{0} has value {1} which is not in the range [{2}, {3}].", name, value, min, max));
			return value;
		}

		public static int ReadIntInRange(this IReadBuffer buffer, int min, int max, string name)
		{
			var value = buffer.ReadInt();
			if(value < min || value > max)
				throw new KafkaException(string.Format("{0} has value {1} which is not in the range [{2}, {3}].", name, value, min, max));
			return value;
		}

		public static long ReadLongInRange(this IReadBuffer buffer, long min, long max, string name)
		{
			var value = buffer.ReadLong();
			if(value < min || value > max)
				throw new KafkaException(string.Format("{0} has value {1} which is not in the range [{2}, {3}].", name, value, min, max));
			return value;
		}
	}
}