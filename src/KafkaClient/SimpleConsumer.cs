using System;
using System.Net.Sockets;
using System.Reflection;
using Common.Logging;
using KafkaClient.Api;
using KafkaClient.IO;
using KafkaClient.Network;

namespace KafkaClient
{
	public class SimpleConsumer
	{
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();

		private readonly string _host;
		private readonly int _port;
		private readonly int _soTimeout;
		private readonly int _bufferSize;
		private readonly string _clientId;
		private readonly Channel _channel;
		private readonly string _brokerInfo;
		private bool _isClosed;
		private readonly object _lock = new object();

		public SimpleConsumer(string host, int port, int soTimeout, int bufferSize, string clientId)
		{
			_host = host;
			_port = port;
			_soTimeout = soTimeout;
			_bufferSize = bufferSize;
			_clientId = clientId;
			_channel = new Channel(host, port, bufferSize, Channel.UseDefaultBufferSize, soTimeout);
			_brokerInfo = string.Format("host_{0}-port_{1}", host, port);

		}

		private void Connect()
		{
			Close();
			_channel.Connect();
		}

		private void Disconnect()
		{
			if(_channel.IsConnected)
			{
				_Logger.DebugFormat("Disconnecting from {0}:{1}", _host, _port);
				_channel.Disconnect();
			}
		}

		private void Reconnect()
		{
			Disconnect();
			Connect();
		}

		public void Close()
		{
			lock(_lock)
			{
				Disconnect();
				_isClosed = true;
			}
		}

		private byte[] SendRequestAndReadResponse(RequestOrResponse msg, int numberOfRetries=1)
		{
			Func<RequestOrResponse, byte[]> sendAndReceive = m =>
			{
				_channel.Send(m);
				return _channel.Receive();
			};

			var numberOfTriesLeft = numberOfRetries + 1;
			lock(_lock)
			{
				GetOrMakeConnection();
				while(numberOfTriesLeft > 0)
				{
					try
					{
						var response = sendAndReceive(msg);
						return response;
					}
					catch(Exception ex)
					{
						if(ex is ErrorReadingException || ex is SocketException)
						{
							numberOfTriesLeft--;
							if(numberOfTriesLeft > 0)
							{
								Reconnect();
								_Logger.Info("Reconnect due to socket error: " + ex.Message);
							}
							else
								throw;
						}
						else
							throw;
					}
				}
			}
			throw new InvalidOperationException("This should be unreachable. NumberOfRetries="+numberOfRetries);
		}

		public FetchResponse Fetch(FetchRequest request)
		{
			var response = SendRequestAndReadResponse(request);
			var readBuffer = new ReadBuffer(response);
			var fetchResponse = FetchResponse.Deserialize(readBuffer);
			return fetchResponse;
		}


		private void GetOrMakeConnection()
		{
			if(!_isClosed && !_channel.IsConnected)
			{
				Connect();
			}
		}

	}
}