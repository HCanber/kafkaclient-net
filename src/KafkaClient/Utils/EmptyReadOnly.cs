using System;
using System.Collections;
using System.Collections.Generic;

namespace Kafka.Client.Utils
{
	public class EmptyReadOnly<T> : IReadOnlyList<T>
	{
		private static readonly EmptyReadOnly<T> _Instance = new EmptyReadOnly<T>();
		private static readonly EmtpyEnumerator _EnumeratorInstance=new EmtpyEnumerator();

		public IEnumerator<T> GetEnumerator()
		{
			return _EnumeratorInstance;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public int Count { get { return 0; } }

		public static EmptyReadOnly<T> Instance { get { return _Instance; } }

		public T this[int index] { get { throw new IndexOutOfRangeException(); } }

		private class EmtpyEnumerator : IEnumerator<T>
		{
			public void Dispose()
			{
				//Intentionally left blank
			}

			public bool MoveNext()
			{
				return false;
			}

			public void Reset()
			{
				//Intentionally left blank
			}

			public T Current { get { throw new InvalidOperationException(); } }

			object IEnumerator.Current
			{
				get { return Current; } }
		}
	}
}