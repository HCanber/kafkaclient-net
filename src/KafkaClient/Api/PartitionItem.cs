namespace Kafka.Client.Api
{
	public static class PartitionItem
	{
		public static PartitionItem<T> Create<T>(int partition, T item)
		{
			return new PartitionItem<T>(partition, item);
		}
	}

	public class PartitionItem<T>
	{
		private readonly int _partition;
		private readonly T _item;

		public PartitionItem(int partition, T item)
		{
			_partition = partition;
			_item = item;
		}

		public int Partition { get { return _partition; } }

		public T Item { get { return _item; } }
	}
}