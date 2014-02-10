using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Kafka.Client.IO;
using Kafka.Client.JetBrainsAnnotations;
using Kafka.Client.Utils;

namespace Kafka.Client.Api
{
	public enum RequiredAck
	{
		WrittenByAllReplicasInSync=-1,
		FireAndForget=0,
		WrittenToDiskByLeader=1,
	}

	public class ProduceRequest : RequestBase
	{
		private readonly short _requiredAcks;
		private readonly int _ackTimeoutMs;
		private List<TopicItem<List<PartitionItem<List<MessageSetItem>>>>> _topicItems;

		public ProduceRequest(IEnumerable<KeyValuePair<TopicAndPartition, IEnumerable<IMessage>>> messagesForPartitions, RequiredAck requiredAcks = RequiredAck.WrittenToDiskByLeader, int ackTimeoutMs = 1000)
			: this(messagesForPartitions,(short)requiredAcks,ackTimeoutMs)
		{
			//Intentionally left blank
		}

		public ProduceRequest([NotNull] IEnumerable<KeyValuePair<TopicAndPartition, IEnumerable<IMessage>>> messagesForPartitions, short requiredAcks, int ackTimeoutMs = 1000)
			: base((short) RequestApiKeys.Produce)
		{
			if(messagesForPartitions == null) throw new ArgumentNullException("messagesForPartitions");
			if(requiredAcks<-1) throw new ArgumentOutOfRangeException("requiredAcks", "RequiredAcks must be >=-1. Actual value="+requiredAcks);
			if(ackTimeoutMs<0) throw new ArgumentOutOfRangeException("ackTimeoutMs", "AckTimeout must be >0. Actual value="+ackTimeoutMs);

			_requiredAcks = requiredAcks;
			_ackTimeoutMs = ackTimeoutMs;
			const int offsetValueIsIgnored = -1;
			_topicItems = messagesForPartitions.GroupBy(kvp => kvp.Key.Topic, kvp => kvp, 
				(topic, items) => TopicItem.Create(topic,
					items.Select(item => PartitionItem.Create(item.Key.Partition, item.Value.Select(m => new MessageSetItem(offsetValueIsIgnored, m)).ToList())).ToList()
					)).ToList();
		}

		protected override void WriteRequestMessage(KafkaWriter writer)
		{
			writer.WriteShort(_requiredAcks);
			writer.WriteInt(_ackTimeoutMs);

			writer.WriteRepeated(_topicItems, topicItem =>
			{
				writer.WriteShortString(topicItem.Topic);
				writer.WriteRepeated(topicItem.Item, partitionMessage =>
				{
					writer.WriteInt(partitionMessage.Partition);
					var messageSetItems = partitionMessage.Item;
					var messageSetSize = GetMessageSetSize(messageSetItems);
					writer.WriteInt(messageSetSize);
					foreach(var messageSetItem in messageSetItems)
					{
						messageSetItem.WriteTo(writer);
					}
				});
			});
		}

		private static int GetMessageSetSize(List<MessageSetItem> messageSetItems)
		{
			return messageSetItems.Sum(m=>(m.GetSize()));
		}

		protected override int MessageSizeInBytes
		{
			get
			{
				var shortSize = BitConversion.ShortSize;
				var intSize = BitConversion.IntSize;
				return shortSize + //RequiredAcks
				       intSize + //Timeout
				       KafkaWriter.GetArraySize(_topicItems, topicItem =>
				       {
					       return KafkaWriter.GetShortStringLength(topicItem.Topic) +
					              KafkaWriter.GetArraySize(topicItem.Item, partitionMessage =>
					              {
						              return intSize + //PartitionId
						                     intSize //MessageSetSize
						                     + GetMessageSetSize(partitionMessage.Item);
					              });
				       });
			}
		}
	}
}