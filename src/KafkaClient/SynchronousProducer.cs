//using Common.Logging;
//using KafkaClient.Network;

//namespace KafkaClient
//{
//	public class SynchronousProducer
//	{
//		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();
//		private readonly object _lock = new object();
		 
//		public SimpleConsumer(string host, int port, int soTimeout, int bufferSize, string clientId)
//		{
//			_host = host;
//			_port = port;
//			_soTimeout = soTimeout;
//			_bufferSize = bufferSize;
//			_clientId = clientId;
//			_channel = new Channel(host, port, bufferSize, Channel.UseDefaultBufferSize, soTimeout);
//			_brokerInfo = string.Format("host_{0}-port_{1}", host, port);

//		}
//	}
//}