using System;
using System.ComponentModel;
using System.Text;
using Kafka.Client.Utils;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.TestHelpers
{
	public static class ByteExtensions
	{
		public static void ShouldBeString(this ArraySegment<byte> actual, string expected)
		{
			if(actual.Count != expected.Length)
			{
				throw new Exception(string.Format("Expected \"{0}\" but array is to short.\nExpected length: {1}\nActual length:     {2}", expected, expected.Length,actual.Count));
			}
			ShouldBeString(actual.Array, actual.Offset, expected);
		}

		public static void ShouldBeString(this byte[] actual, string expected)
		{
			if(actual.Length != expected.Length)
			{
				throw new Exception(string.Format("Expected \"{0}\" but array is to short.\nExpected length: {1}\nActual length:     {2}", expected, expected.Length, actual.Length));
			}
			ShouldBeString(actual, 0, expected);
		}

		public static void ShouldBeString(this byte[] actual, int startIndex, string expected)
		{
			var len = expected.Length;

			var requiredLength = startIndex + len;
			var actualLength = actual.Length;
			if(requiredLength > actualLength) throw new Exception(string.Format("Expected \"{0}\" at index {1}, but array is to short.\nExpected length: >={2}\nActual length:     {3}", expected, startIndex, requiredLength, actualLength));
			for(var i = 0; i < len; i++)
			{
				var actualChar = actual[startIndex + i];
				var expectedChar = expected[i];
				if(actualChar != (byte)expectedChar)
				{
					throw new Exception(string.Format("Expected \"{0}\" at index {1}, but they differ at character {2} (index {7}).\nExpected: \"{3}\" ({4})\nActual:   \"{5}\" ({6})", expected, startIndex, i + 1, expectedChar, (int)expectedChar, actualChar, (int)actualChar, i + startIndex));
				}
			}
		}

		public static void ShouldBeShortString(this byte[] actual, ref int startIndex, string expected)
		{
			var actualLength = GetShort(actual,ref startIndex);
			if(actualLength == -1)
			{
				expected.ShouldBeNull();
			}
			else
			{
				var expectedLength = expected.Length;

				var requiredLength = startIndex + expectedLength;
				if(requiredLength > actual.Length) throw new Exception(string.Format("Expected \"{0}\" at index {1}, but array is to short.\nExpected length: >={2}\nActual length:     {3}", expected, startIndex, requiredLength, actual.Length));
				for(var i = 0; i < expectedLength; i++)
				{
					var actualChar = actual[startIndex + i];
					var expectedChar = expected[i];
					if(actualChar != (byte)expectedChar)
					{
						throw new Exception(string.Format("Expected \"{0}\" at index {1}, but they differ at character {2} (index {7}).\nExpected: \"{3}\" ({4})\nActual:   \"{5}\" ({6})", expected, startIndex, i + 1, expectedChar, (int)expectedChar, actualChar, (int)actualChar, i + startIndex));
					}
				}				
				Assert.True(actualLength == expected.Length, "Too long string.\nExpected: \"" + expected + "\"\n  Actual: \"" + Encoding.UTF8.GetString(actual, startIndex, actualLength) + "\"");
				startIndex += expectedLength;
			}
		}

		public static T Get<T>(this byte[] bytes, Func<byte[], int, T> getValueFromBytes, ref int index, int length)
		{
			var value = getValueFromBytes(bytes, index);
			index += length;
			return value;
		}

		public static void ShouldBeBytes(this byte[] actual, ref int index, string expected)
		{
			var expectedBytes =expected==null ? null: Encoding.UTF8.GetBytes(expected);
			ShouldBeBytes(actual, ref index, expectedBytes);
		}

		public static void ShouldBeBytes(this byte[] actual, ref int index, byte[] expected)
		{
			var length = GetInt(actual,ref index);

			var expectedLength =expected==null? -1: expected.Length;
			Assert.True(length==expectedLength, "Expected length: "+expectedLength+"  Actual length:"+length);
			if(length == -1) return;
			var bytes=Get(actual, (bs, i) =>
			{
				var result = new byte[length];
				Array.Copy(bs,i,result,0,length);
				return result;
			}, ref index, length);
			bytes.ShouldOnlyContainInOrder(expected);
		}

		public static byte GetByte(this byte[] bytes, ref int index)
		{
			return Get(bytes,(bs, i) =>bs[i] , ref index,1);
		}

		public static short GetShort(this byte[] bytes, ref int index)
		{
			return Get(bytes, BitConversion.GetShortFromBigEndianBytes, ref index, BitConversion.ShortSize);
		}

		public static int GetInt(this byte[] bytes, ref int index)
		{
			return Get(bytes, BitConversion.GetIntFromBigEndianBytes, ref index, BitConversion.IntSize);
		}

		public static uint GetGetUintLong(this byte[] bytes, ref int index)
		{
			return Get(bytes, BitConversion.GetUIntFromBigEndianBytes, ref index, BitConversion.IntSize);
		}

		public static long GetLong(this byte[] bytes, ref int index)
		{
			return Get(bytes, BitConversion.GetLongFromBigEndianBytes, ref index, BitConversion.LongSize);
		}

	}
}