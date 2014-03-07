namespace Kafka.Client.Exceptions
{
	/// <summary>
	/// A marker interface for exceptions that are thrown for errors that typically resolves after a while.
	/// <example>Example: when getting meta data for a topic that do not exists, but Kafka's config has autoCreateTopicsEnable=true set,
	/// the topic is created, but no leader has yet been selected. In that case a <see cref="TopicCreatedNoLeaderYetException"/> is thrown.
	/// This is a retryable exception, so the client should wait a while and then retry.</example>
	/// </summary>
	public interface IRetryableError { }
}