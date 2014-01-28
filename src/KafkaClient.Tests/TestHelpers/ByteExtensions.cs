using System;
using Xunit.Should;

namespace KafkaClient.Tests.TestHelpers
{
	public static class ByteExtensions
	{
		public static void ShouldBeString(this byte[] bytes, int startIndex, string s)
		{
			var len = s.Length;

			var requiredLength = startIndex + len;
			var actualLength = bytes.Length;
			if(requiredLength > actualLength) throw new Exception(string.Format("Expected \"{0}\" at index {1}, but array is to short.\nExpected length: >={2}\nActual length:     {3}", s, startIndex, requiredLength, actualLength));
			for(var i = 0; i < len; i++)
			{
				var actual = bytes[startIndex + i];
				var expected = s[i];
				if(actual != (byte)expected)
				{
					throw new Exception(string.Format("Expected \"{0}\" at index {1}, but they differ at character {2} (index {7}).\nExpected: \"{3}\" ({4})\nActual:   \"{5}\" ({6})", s, startIndex, i + 1, expected, (int)expected, actual, (int)actual, i+startIndex));
				}
			}
		}
	}
}