using KafkaClient.IO;

namespace KafkaClient.Api
{
	public class Broker
	{
		private readonly int _nodeId;
		private readonly string _host;
		private readonly int _port;

		public Broker(int nodeId, string host, int port)
		{
			_nodeId = nodeId;
			_host = host;
			_port = port;
		}

		public int NodeId
		{
			get { return _nodeId; }
		}

		public string Host
		{
			get { return _host; }
		}

		public int Port
		{
			get { return _port; }
		}

		public static Broker Deserialize(IReadBuffer buffer)
		{
			var nodeId = buffer.ReadInt();
			var host = buffer.ReadShortString();
			var port = buffer.ReadInt();
			return new Broker(nodeId,host,port);
		}
	}
}