using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaClient.Utils;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests
{
	public class BitConversionTests
	{
		[Fact]
		public void GettingShortFromBytesTest()
		{
			var value = BitConversion.GetShortFromBigEndianBytes(new byte[] { 0x0f, 0x02, 0x03 }, 1);
			value.ShouldBe((short)0x0203);
		}

		[Fact]
		public void GettingIntFromBytesTest()
		{
			var value = BitConversion.GetIntFromBigEndianBytes(new byte[] { 0x0f, 0x02, 0x03, 0x04, 0x05 }, 1);
			value.ShouldBe(0x02030405);
		}

		[Fact]
		public void GettingLongFromBytesTest()
		{
			var value = BitConversion.GetLongFromBigEndianBytes(new byte[] { 0x0f, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09 }, 1);
			value.ShouldBe(0x0203040506070809L);
		}

		[Fact]
		public void GetBigEndianBytesFromShortTest()
		{
			var bytes = BitConversion.GetBigEndianBytes((short)0x1234);
			bytes.ShouldOnlyContainInOrder<byte>(0x12, 0x34);
		}

		[Fact]
		public void GetBigEndianBytesFromIntTest()
		{
			var bytes = BitConversion.GetBigEndianBytes(0x12345678);
			bytes.ShouldOnlyContainInOrder<byte>(0x12, 0x34, 0x56, 0x78);
		}

		[Fact]
		public void GetBigEndianBytesFromLongTest()
		{
			var bytes = BitConversion.GetBigEndianBytes(0x123456789abcdef0L);
			bytes.ShouldOnlyContainInOrder<byte>(0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0);
		}

		[Fact]
		public void ReverseBytesTest()
		{
			var bytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
			BitConversion.ReverseBytes(bytes);
			bytes.ShouldOnlyContainInOrder<byte>(9, 8, 7, 6, 5, 4, 3, 2, 1);
		}

		[Fact]
		public void ReverseBytesTest2()
		{
			var bytes = new byte[] { 0xff, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xbb };
			BitConversion.ReverseBytes(bytes,1,9);
			bytes.ShouldOnlyContainInOrder<byte>(0xff, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0xbb);
		}
	}
}
