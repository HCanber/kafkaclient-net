namespace Kafka.Client.Api
{
	public class PartitionFetchInfo
	{
		private readonly long _offset;
		private readonly int _fetchSize;

		public PartitionFetchInfo(long offset, int fetchSize)
		{
			_offset = offset;
			_fetchSize = fetchSize;
		}

		public long Offset
		{
			get { return _offset; }
		}

		public int FetchSize
		{
			get { return _fetchSize; }
		}
	}
}