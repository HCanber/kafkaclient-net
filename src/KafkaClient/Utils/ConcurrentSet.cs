using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Kafka.Client.Utils
{
	public class ConcurrentSet<T> : IReadOnlyCollection<T>
	{
		private readonly ConcurrentDictionary<T, byte> _dictionary = new ConcurrentDictionary<T, byte>();

		public int Count { get { return _dictionary.Count; } }

		public bool TryAdd(T item)
		{
			return _dictionary.TryAdd(item, 0);
		}

		public bool Contains(T item)
		{
			return _dictionary.ContainsKey(item);
		}

		public bool TryRemove(T item)
		{
			byte value;
			return _dictionary.TryRemove(item, out value);
		}

		public void Clear()
		{
			_dictionary.Clear();
		}
		


		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public IEnumerator<T> GetEnumerator()
		{
			return _dictionary.Keys.GetEnumerator();
		}
	}
}