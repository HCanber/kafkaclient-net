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
			if(sequence != null)
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

		public static Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(this IEnumerable<KeyValuePair<TKey, TValue>> sequence)
		{
			return sequence.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
		}

		public static ReadOnlyDictionary<TKey, TValue> ToReadonly<TKey, TValue>(this IDictionary<TKey, TValue> dictionary)
		{
			return new ReadOnlyDictionary<TKey, TValue>(dictionary);
		}

		public static IReadOnlyCollection<T> ToImmutable<T>(this IEnumerable<T> sequence)
		{
			return new ReadOnlyCollection<T>(sequence.ToList());
		}

		public static IReadOnlyList<T> ToImmutableList<T>(this IEnumerable<T> sequence)
		{
			return new ReadOnlyCollection<T>(sequence.ToList());
		}

		public static Dictionary<TKey, IReadOnlyCollection<TValue>> GroupByToReadOnlyCollectionDictionary<TKey, TValue>(this IEnumerable<TValue> sequence, Func<TValue, TKey> keySelector, Func<TValue, TKey, bool> shouldBeIncluded = null, Action<TValue, TKey> handleNotIncluded = null)
		{
			return GroupByToReadOnlySequenceDictionary<TValue, TKey, TValue, IReadOnlyCollection<TValue>>(sequence, keySelector, (key, value) => value, shouldBeIncluded, handleNotIncluded);
		}

		public static Dictionary<TKey, IReadOnlyCollection<TValue>> GroupByToReadOnlyCollectionDictionary<TSource, TKey, TValue>(this IEnumerable<TSource> sequence, Func<TSource, TKey> keySelector, Func<TSource, TValue> valueSelector, Func<TSource, TKey, bool> shouldBeIncluded = null, Action<TSource, TKey> handleNotIncluded = null)
		{
			return GroupByToReadOnlySequenceDictionary<TSource, TKey, TValue, IReadOnlyCollection<TValue>>(sequence, keySelector, (key, value) => valueSelector(value), shouldBeIncluded, handleNotIncluded);

		}

		public static Dictionary<TKey, IReadOnlyList<TValue>> GroupByToReadOnlyListDictionary<TKey, TValue>(this IEnumerable<TValue> sequence, Func<TValue, TKey> keySelector, Func<TValue, TKey, bool> shouldBeIncluded = null, Action<TValue, TKey> handleNotIncluded = null)
		{
			return GroupByToReadOnlySequenceDictionary<TValue, TKey, TValue, IReadOnlyList<TValue>>(sequence, keySelector, (key, value) => value, shouldBeIncluded, handleNotIncluded);
		}

		public static Dictionary<TKey, IReadOnlyList<TValue>> GroupByToReadOnlyListDictionary<TSource, TKey, TValue>(this IEnumerable<TSource> sequence, Func<TSource, TKey> keySelector, Func<TSource, TValue> valueSelector, Func<TSource, TKey, bool> shouldBeIncluded = null, Action<TSource, TKey> handleNotIncluded = null)
		{
			return GroupByToReadOnlySequenceDictionary<TSource, TKey, TValue, IReadOnlyList<TValue>>(sequence, keySelector, (key, value) => valueSelector(value), shouldBeIncluded, handleNotIncluded);
		}


		private static Dictionary<TKey, TCollection> GroupByToReadOnlySequenceDictionary<TSource, TKey, TValue, TCollection>(this IEnumerable<TSource> sequence, Func<TSource, TKey> keySelector, Func<TKey, TSource, TValue> valueSelector, Func<TSource, TKey, bool> shouldBeIncluded, Action<TSource, TKey> handleNotIncluded) where TCollection : class, IReadOnlyCollection<TValue>
		{
			var dictionary = new Dictionary<TKey, TCollection>();
			var hasShouldBeIncluded = shouldBeIncluded != null;
			var shouldHandleNotIncluded = handleNotIncluded != null;

			foreach(var item in sequence)
			{
				var key = keySelector(item);
				if(!hasShouldBeIncluded || shouldBeIncluded(item, key))
				{
					var value = valueSelector(key, item);
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
				else
				{
					if(shouldHandleNotIncluded)
					{
						handleNotIncluded(item, key);
					}
				}
			}
			return dictionary;
		}

		private class ReadOnly<T> : ReadOnlyCollection<T>
		{
			public ReadOnly(IList<T> list)
				: base(list)
			{
			}
			public IList<T> Values { get { return Items; } }
		}
	}
}