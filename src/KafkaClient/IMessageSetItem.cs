namespace Kafka.Client
{
	public interface IMessageSetItem
	{
		long Offset { get; }
		IMessage Message { get; }
	}
}