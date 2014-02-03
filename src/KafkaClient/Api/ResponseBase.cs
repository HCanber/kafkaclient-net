namespace Kafka.Client.Api
{
	public abstract class ResponseBase
	{
		private readonly int _correlationId;

		protected ResponseBase(int correlationId)
		{
			_correlationId = correlationId;
		}

		public int CorrelationId { get { return _correlationId; } }
	}
}