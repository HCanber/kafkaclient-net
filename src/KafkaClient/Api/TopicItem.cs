using System.Collections;
using System.Collections.Generic;

namespace Kafka.Client.Api
{
	public static class TopicItem
	{
		public static TopicItem<T> Create<T>(string topic, T item)
		{
			return new TopicItem<T>(topic,item);
		}
	}

	public class TopicItem<T>
	{
		private readonly string _topic;
		private readonly T _item;

		public TopicItem(string topic, T item)
		{
			_topic = topic;
			_item = item;
		}

		public string Topic { get { return _topic; } }
		public T Item { get { return _item; } }

	}
}