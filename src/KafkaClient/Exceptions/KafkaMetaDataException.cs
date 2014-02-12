using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Kafka.Client.Api;
using Kafka.Client.Utils;

namespace Kafka.Client.Exceptions
{
	[Serializable]
	public class KafkaMetaDataException : KafkaException
	{
		private readonly IReadOnlyList<TopicItem<KafkaError>> _topicErrors;
		private readonly IReadOnlyList<TopicAndPartitionValue<KafkaError>> _partitionErrors;

		public KafkaMetaDataException(IReadOnlyList<TopicItem<KafkaError>> topicErrors, string message)
			: this(topicErrors, EmptyReadOnly<TopicAndPartitionValue<KafkaError>>.Instance, message)
		{
		}

		public KafkaMetaDataException(IReadOnlyList<TopicItem<KafkaError>> topicErrors, IReadOnlyList<TopicAndPartitionValue<KafkaError>> partitionErrors, string message)
			: base(message)
		{
			_topicErrors = topicErrors;
			_partitionErrors = partitionErrors;
		}

		public IReadOnlyList<TopicItem<KafkaError>> TopicErrors { get { return _topicErrors; } }

		public IReadOnlyList<TopicAndPartitionValue<KafkaError>> PartitionErrors { get { return _partitionErrors; } }

		public override string ToString()
		{
			var sb = new StringBuilder();
			var message = base.ToString();
			sb.Append(message);
			if(message.Length>0) sb.Append(Environment.NewLine);

			AppendList(sb, TopicErrors.OrderBy(t => t.Topic).ToList(), "Topic errors: ");
			sb.Append(Environment.NewLine);
			AppendList(sb, PartitionErrors.OrderBy(t => t.TopicAndPartition).ToList(), "Topic errors: ");
			return sb.ToString();
		}

		private static void AppendList<T>(StringBuilder sb, IReadOnlyList<T> list, string description)
		{
			sb.Append(description);
			sb.Append('[');
			var enumerator = list.GetEnumerator();
			if(enumerator.MoveNext())
			{
				sb.Append(enumerator.Current);
				while(enumerator.MoveNext())
				{
					sb.Append(", ");
					sb.Append(enumerator.Current);
				}
			}
			sb.Append(']');
		}

		protected KafkaMetaDataException(SerializationInfo info, StreamingContext context)
			: base(info, context)
		{
		}
	}
}