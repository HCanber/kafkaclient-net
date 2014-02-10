using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Kafka.Client.Utils
{
	public static class CollectionExtensions
	{
		public static void ForEach<T>(this IEnumerable<T> sequence, Action<T> action)
		{
			if(sequence!=null)
			{
				foreach(var item in sequence)
				{
					action(item);
				}
			}
		}

		public static void AddToList<TKey, TValue>(this Dictionary<TKey, List<TValue>> dictionary, TKey key, TValue value)
		{
			List<TValue> list;
			if(!dictionary.TryGetValue(key, out list))
			{
				list = new List<TValue>();
				dictionary.Add(key, list);
			}
			list.Add(value);
		}

		public static IReadOnlyCollection<T> ToImmutable<T>(this IEnumerable<T> sequence)
		{
			return new ReadOnlyCollection<T>(sequence.ToList());
		}

		public static Dictionary<TKey, IReadOnlyCollection<TValue>> GroupByToReadOnlyCollectionDictionary<TKey, TValue>(this IEnumerable<TValue> sequence, Func<TValue, TKey> keySelector)
		{
			return GroupByToReadOnlySequenceDictionary<TValue, TKey, TValue, IReadOnlyCollection<TValue>>(sequence, keySelector, value => value);
		}

		public static Dictionary<TKey, IReadOnlyCollection<TValue>> GroupByToReadOnlyCollectionDictionary<TSource, TKey, TValue>(this IEnumerable<TSource> sequence, Func<TSource, TKey> keySelector, Func<TSource, TValue> valueSelector)
		{
			return GroupByToReadOnlySequenceDictionary<TSource, TKey, TValue, IReadOnlyCollection<TValue>>(sequence, keySelector, valueSelector);

		}

		public static Dictionary<TKey, IReadOnlyList<TValue>> GroupByToReadOnlyListDictionary<TKey, TValue>(this IEnumerable<TValue> sequence, Func<TValue, TKey> keySelector)
		{
			return GroupByToReadOnlySequenceDictionary<TValue, TKey, TValue, IReadOnlyList<TValue>>(sequence, keySelector, value=>value);
		}

		public static Dictionary<TKey, IReadOnlyList<TValue>> GroupByToReadOnlyListDictionary<TSource, TKey, TValue>(this IEnumerable<TSource> sequence, Func<TSource, TKey> keySelector, Func<TSource, TValue> valueSelector)
		{
			return GroupByToReadOnlySequenceDictionary<TSource, TKey, TValue, IReadOnlyList<TValue>>(sequence, keySelector, valueSelector);
		}


		private static Dictionary<TKey, TCollection> GroupByToReadOnlySequenceDictionary<TSource, TKey, TValue, TCollection>(this IEnumerable<TSource> sequence, Func<TSource, TKey> keySelector, Func<TSource, TValue> valueSelector) where TCollection: class, IReadOnlyCollection<TValue>
		{
			var dictionary = new Dictionary<TKey, TCollection>();
			foreach(var item in sequence)
			{
				var key = keySelector(item);
				var value = valueSelector(item);
				TCollection collection;
				if(dictionary.TryGetValue(key, out collection))
				{
					var readOnly = collection as ReadOnly<TValue>;
					// ReSharper disable once PossibleNullReferenceException	
					readOnly.Values.Add(value);
				}
				else
				{
					dictionary.Add(key, new ReadOnly<TValue>(new List<TValue> { value }) as TCollection);
				}
			}
			return dictionary;
		}

		private class ReadOnly<T> : ReadOnlyCollection<T>
		{
			public ReadOnly(IList<T> list) : base(list)
			{
			}
			public IList<T> Values { get { return Items; } }
		}
	}
}