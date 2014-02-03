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
				list=new List<TValue>();
				dictionary.Add(key,list);
			}
			list.Add(value);
		}

		public static IReadOnlyCollection<T> ToImmutable<T>(this IEnumerable<T> sequence)
		{
			return new ReadOnlyCollection<T>(sequence.ToList());
		}
	}
}