using System;
using Kafka.Client.Api;
using Kafka.Client.Utils;
using KafkaClient.Tests.TestHelpers;
using Xunit.Should;

namespace KafkaClient.Tests.Api
{
	public static class MessagePartsValidator
	{
		public static void ShouldMatchRequestMessageHeader(this byte[] bytes,ref int index, int correlationId, string clientId, RequestApiKeys apiKey)
		{
			bytes.GetShort(ref index).ShouldBe<short>((short)apiKey);	//ApiKey=FetchRequest
			bytes.GetShort(ref index).ShouldBe<short>(0);	//ApiVersion
			bytes.GetInt(ref index).ShouldBe(correlationId);	//CorrelationId
			bytes.ShouldBeShortString(ref index, clientId);                       //ClientId
		}

		public static void ShouldBeMessageSetItem(this byte[] bytes, ref int index, string key, string value, int? crc=null, long? offset=null, byte magicByte=0, byte attributes=0)
		{
			var messageSize = CalculateMessageSize(key, value);
			if(offset.HasValue)
				bytes.GetLong(ref index).ShouldBe(offset.Value);	          //Offset
			else
				index += BitConversion.LongSize;                            //Skip Offset, its' value can be anything

			bytes.GetInt(ref index).ShouldBe(messageSize);	              //MessageSize
			ShouldBeMessage(bytes, ref index, key, value, crc, magicByte, attributes);
		}

		public static void ShouldBeMessage(this byte[] bytes, ref int index, string key, string value, int? crc, byte magicByte, byte attributes)
		{
			if(crc.HasValue)
				bytes.GetInt(ref index).ShouldBe(crc.Value); //Crc
			else
				index += BitConversion.IntSize; //Skip CRC

			bytes.GetByte(ref index).ShouldBe<byte>(magicByte); //MagicByte
			bytes.GetByte(ref index).ShouldBe<byte>(attributes); //Attributes
			bytes.ShouldBeBytes(ref index, key); //Key
			bytes.ShouldBeBytes(ref index, value); //Value
		}

		public static int CalculateMessageSize(string key, string value)
		{
			var keyLength = key == null ? 0 : key.Length;
			var valueLength = value == null ? 0 : value.Length;
			var messageSize = 4 + 1 + 1 + (4 + keyLength) + (4 + valueLength);    //   Crc + MagicByte + Attributes + Key + Value
			return messageSize;
		}
	}
}