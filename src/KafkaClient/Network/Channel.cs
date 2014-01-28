using System;
using System.Net.Sockets;
using Common.Logging;
using KafkaClient.Api;
using KafkaClient.IO;
using KafkaClient.Utils;

namespace KafkaClient.Network
{
	public class Channel
	{
		public const int UseDefaultBufferSize = -1;
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();


		private readonly string _host;
		private readonly int _port;
		private readonly int _readBufferSize;
		private readonly int _writeBufferSize;
		private readonly int _readTimeoutMs;
		private bool _isConnected;
		private readonly TcpClient _client;
		private readonly byte[] _sizeBuffer = new byte[BitConversion.IntSize];

		public Channel(string host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs)
		{
			_host = host;
			_port = port;
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
			//socket.Blocking = true;
			socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
		}

		public bool IsConnected { get { return _isConnected; } }

		public void Connect()
		{
			if(_isConnected) return;
			if(_isConnected) return;
			_client.Connect(_host, _port);
			_isConnected = true;
			if(_Logger.IsDebugEnabled)
			{
				_Logger.Debug(string.Format("Created socket with ReceiveTimeout = {0} (requested {1}), ReceiveBufferSize = {2} (requested {3}), SendBufferSize = {4} (requested {5}).", _client.ReceiveTimeout, _readTimeoutMs, _client.ReceiveBufferSize, _readBufferSize, _client.SendBufferSize, _writeBufferSize));
			}
		}

		public void Disconnect()
		{
			if(!_isConnected) return;
			if(!_isConnected) return;
			Swallow(() => _client.Close());
			_isConnected = false;
		}

		public int Send(RequestOrResponse msg)
		{
			if(!_isConnected)
				throw new ClosedChannelException();
			var buffer = WriteBuffer.Create(msg);
			var networkStream = _client.GetStream();
			buffer.WriteTo(networkStream);
			return buffer.Size;
		}

		public byte[] Receive()
		{
			if(!_isConnected)
				throw new ClosedChannelException();
			var networkStream = _client.GetStream();

			//Start by reading first 4 bytes to get the size of the rest of the message
			ReadFromStream(networkStream, _sizeBuffer, 0, BitConversion.IntSize);
			var size = BitConversion.GetIntFromBigEndianBytes(_sizeBuffer, 0);

			var buffer = ReadFromStream(networkStream, size);
			return buffer;
		}

		private static byte[] ReadFromStream(NetworkStream networkStream, int size, int startIndex = 0)
		{
			var buffer = new byte[size];
			ReadFromStream(networkStream, buffer, startIndex, size);
			return buffer;
		}

		private static void ReadFromStream(NetworkStream networkStream, byte[] buffer, int startIndex, int length)
		{
			var bytesRead = 0;
			while(bytesRead < length && networkStream.CanRead)
			{
				bytesRead += networkStream.Read(buffer, startIndex + bytesRead, length - bytesRead);
			}
			if(bytesRead < length)
			{
				throw new ErrorReadingException(string.Format("Did not receive everything. Expected {0} bytes. Only received {1} bytes.", length, bytesRead));
			}
		}


		private void Swallow(Action action)
		{
			_Logger.SwallowAsWarning(action);
		}
	}
}