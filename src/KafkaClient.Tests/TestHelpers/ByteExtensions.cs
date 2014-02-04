using System;
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
	}
}