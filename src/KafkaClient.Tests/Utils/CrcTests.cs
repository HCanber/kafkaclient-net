using KafkaClient.Utils;
using Xunit;
using Xunit.Should;

namespace KafkaClient.Tests.Utils
{
	public class CrcTests
	{
		[Fact]
		public void Calculates_correctly()
		{
			var bytes = new byte[]
			{
				0x00, 											 // MagicByte
				0x00, 											 // Attributes
				0xFF, 0xFF, 0xFF, 0xFF, 		 // Key, length = -1
				0x00, 0x00, 0x00, 0x03, 		 // Value, length
				0x48, 0x69, 0x21, 					 // Value "Hi!"
			};
			Crc32.Compute(bytes).ShouldBe(0x0F66BBB7U);
		} 
	}
}