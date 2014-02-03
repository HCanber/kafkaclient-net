using System;
using System.Net.Sockets;
using System.Threading;
using Common.Logging;
using Kafka.Client.IO;
using Kafka.Client.Utils;

namespace Kafka.Client.Network
{
	public class KafkaConnection : IKafkaConnection
	{
		private static int _nextAvailableId = 0;
		public const int UseDefaultBufferSize = -1;
		public const int DefaultReadTimeoutMs = 2 * 60 * 1000;
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();


		private readonly HostPort _hostPort;
		private readonly int _readBufferSize;
		private readonly int _writeBufferSize;
		private readonly int _readTimeoutMs;
		private readonly TcpClient _client;
		private readonly byte[] _sizeBuffer = new byte[BitConversion.IntSize];
		private readonly int _id;

		public KafkaConnection(HostPort hostPort, int readBufferSize = UseDefaultBufferSize, int writeBufferSize = UseDefaultBufferSize, int readTimeoutMs = DefaultReadTimeoutMs)
		{
			_id = Interlocked.Increment(ref _nextAvailableId);
			_hostPort = hostPort;
			_readBufferSize = readBufferSize;
			_writeBufferSize = writeBufferSize;
			_readTimeoutMs = readTimeoutMs;
			_client = new TcpClient
			{
				ReceiveTimeout = readTimeoutMs,
				NoDelay = true
			};
			if(readBufferSize > 0)
				_client.ReceiveBufferSize = readBufferSize;
			if(writeBufferSize > 0)
				_client.SendBufferSize = writeBufferSize;
			var socket = _client.Client;
			socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
			_Logger.DebugFormat("Created connection [{6}] with ReceiveTimeout = {0} (requested {1}), ReceiveBufferSize = {2} (requested {3}), SendBufferSize = {4} (requested {5}).", _client.ReceiveTimeout, _readTimeoutMs, _client.ReceiveBufferSize, _readBufferSize, _client.SendBufferSize, _writeBufferSize, _id);
		}

		public HostPort HostPort{get { return _hostPort; }}

		public bool IsConnected { get { return _client.Connected; }}

		private void Connect()
		{
			if(_client.Connected) return;
			_client.Connect(_hostPort.Host, _hostPort.Port);
			if(_Logger.IsDebugEnabled)
			{
				_Logger.DebugFormat("Connected connection [{0}]", _id);

			}
		}

		public void Disconnect()
		{
			if(!_client.Connected) return;
			Swallow(() => _client.Close(), ex => string.Format("Disconnecting {0} for connection {1} failed.", _hostPort, _id));
			_Logger.DebugFormat("Disconnected connection [{0}]", _id);

		}


		public byte[] Request(IKafkaMessageWriteable msg, int requestId)
		{
			Send(msg, requestId);
			var response = Receive(requestId);
			return response;
		}

		private void Send(IKafkaMessageWriteable msg, int requestId)
		{
			Connect();
			var length = msg.GetSize();
			_Logger.DebugFormat("About to send {2} bytes for request {0} to Kafka {1} on connection [{3}]", requestId, _hostPort, length, _id);
			try
			{
				var networkStream = _client.GetStream();
				var serializedSize = BitConversion.GetBigEndianBytes(msg.GetSize());
				networkStream.Write(serializedSize,0,BitConversion.IntSize);
				msg.WriteTo(networkStream, requestId);
			}
			catch(Exception e)
			{
				_Logger.ErrorException(e, "Unable to send request {0} to Kafka {1} on connection [{2}]", requestId, _hostPort, _id);
				Disconnect();
				throw;
			}
		}


		private byte[] Receive(int requestId)
		{
			Connect();
			_Logger.DebugFormat("Receiving response for request {0} from Kafka {1} on connection [{2}]", requestId, _hostPort, _id);

			try
			{
				var networkStream = _client.GetStream();

				//Start by reading first 4 bytes to get the size of the rest of the message
				ReadFromStream(networkStream, _sizeBuffer, 0, BitConversion.IntSize);
				var size = _sizeBuffer.GetIntFromBigEndianBytes(0);

				var buffer = ReadFromStream(networkStream, size);
				return buffer;
			}
			catch(Exception e)
			{
				_Logger.ErrorException(e, "Unable to receive response for request {0} to Kafka {1} on connection [{2}]", requestId, _hostPort, _id);
				throw;
			}
		}

		private byte[] ReadFromStream(NetworkStream networkStream, int size, int startIndex = 0)
		{
			var buffer = new byte[size];
			ReadFromStream(networkStream, buffer, startIndex, size);
			return buffer;
		}

		private void ReadFromStream(NetworkStream networkStream, byte[] buffer, int startIndex, int length)
		{
			var bytesRead = 0;
			while(bytesRead < length && networkStream.CanRead)
			{								
				bytesRead += networkStream.Read(buffer, startIndex + bytesRead, length - bytesRead);
			}
			if(bytesRead < length)
			{
				throw new ErrorReadingException(string.Format("Did not receive everything. Expected {0} bytes. Only received {1} bytes on connection [{2}].", length, bytesRead, _id));
			}
		}


		private void Swallow(Action action, Func<Exception, string> createMessage)
		{
			_Logger.SwallowAsWarning(action, createMessage);
		}

		public override string ToString()
		{
			return _hostPort.ToString();
		}
	}
}