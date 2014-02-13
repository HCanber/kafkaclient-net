using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Kafka.Client.Exceptions;
using Kafka.Client.IO;
using Kafka.Client.JetBrainsAnnotations;
using Kafka.Client.Utils;

namespace Kafka.Client.Network
{
	public class AsyncKafkaConnection : IAsyncKafkaConnection
	{
		private static int _nextAvailableId;
		public const int UseDefaultBufferSize = -1;
		public const int DefaultReadTimeoutMs = 2 * 60 * 1000;
		private static readonly ILog _Logger = LogManager.GetCurrentClassLogger();
		public const TaskFactory DefaultTaskFactory = null;
		public const TaskScheduler DefaultTaskScheduler = null;

		private readonly HostPort _hostPort;
		private readonly int _readBufferSize;
		private readonly int _writeBufferSize;
		private readonly int _readTimeoutMs;
		private readonly TcpClient _client;
		private readonly byte[] _sizeBuffer = new byte[BitConversion.IntSize];
		private readonly int _id;
		private readonly object _connectLock = new object();
		private readonly ConcurrentDictionary<int, MessageContainer> _messagesByCorrelationId;
		private readonly BlockingCollection<MessageContainer> _sendBlockingCollection;
		private int _correlationId;
		private readonly ManualResetEventSlim _isSendingGate = new ManualResetEventSlim();
		private readonly ManualResetEventSlim _isReceivingSemaphore = new ManualResetEventSlim();
		private readonly ManualResetEventSlim _userHasCalledConnectGate;
		private readonly CancellationTokenSource _cancellationTokenSource;
		private readonly CancellationToken _mainCancellationToken;
		private NetworkStream _networkStream;


		public AsyncKafkaConnection(HostPort hostPort, int readBufferSize = UseDefaultBufferSize, int writeBufferSize = UseDefaultBufferSize, int readTimeoutMs = DefaultReadTimeoutMs, TaskFactory taskFactory = DefaultTaskFactory, TaskScheduler taskScheduler = DefaultTaskScheduler, bool autoConnect = true)
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
			taskFactory = taskFactory ?? Task.Factory;
			_cancellationTokenSource = new CancellationTokenSource();
			_mainCancellationToken = _cancellationTokenSource.Token;
			_sendBlockingCollection = new BlockingCollection<MessageContainer>();
			_messagesByCorrelationId = new ConcurrentDictionary<int, MessageContainer>();
			taskScheduler = taskScheduler ?? TaskScheduler.Default;
			taskFactory.StartNew(ProcessSends, null, _mainCancellationToken, TaskCreationOptions.LongRunning, taskScheduler);
			taskFactory.StartNew(ProcessReceives, null, _mainCancellationToken, TaskCreationOptions.LongRunning, taskScheduler);
			_userHasCalledConnectGate = new ManualResetEventSlim(false);
			_Logger.DebugFormat("Created connection [{6}] to {7} with ReceiveTimeout = {0} (requested {1}), ReceiveBufferSize = {2} (requested {3}), SendBufferSize = {4} (requested {5}).", _client.ReceiveTimeout, _readTimeoutMs, _client.ReceiveBufferSize, _readBufferSize, _client.SendBufferSize, _writeBufferSize, _id, hostPort);
			if(autoConnect)
				_userHasCalledConnectGate.Set();
		}



		public HostPort HostPort { get { return _hostPort; } }

		public bool IsConnected { get { return _client.Connected; } }

		public void Connect()
		{
			ConnectAsync().Wait();
		}

		public Task ConnectAsync()
		{
			//Connect the TcpClient
			var task = _client.ConnectAsync(_hostPort.Host, _hostPort.Port);
			//When connect, signal to the rest of the class that it may continue.
			task.Then(t => _userHasCalledConnectGate.Set());
			return task;
		}

		private NetworkStream EnsureConnectedAndGetStream()
		{
			if(!_client.Connected)
			{
				_userHasCalledConnectGate.Wait(_mainCancellationToken);
				lock(_connectLock)
				{
					if(!_client.Connected)
					{
						InternalConnectDoNotCallThis().Wait(_mainCancellationToken);
					}
				}
			}
			return _networkStream;
		}

		//This should only be called from  EnsureConnected and ConnectAsync
		private async Task InternalConnectDoNotCallThis()
		{
			await _client.ConnectAsync(_hostPort.Host, _hostPort.Port);
			_Logger.DebugFormat("Connected connection [{0}]", _id);
			_networkStream = _client.GetStream();
		}

		public void Disconnect()
		{
			if(!_client.Connected) return;
			lock(_connectLock)
			{
				if(!_client.Connected) return;
				Swallow(() => _client.Close(), ex => string.Format("Disconnecting {0} for connection {1} failed.", _hostPort, _id));
				_Logger.DebugFormat("Disconnected connection [{0}]", _id);
			}
		}

		private void ResetConnection(ManualResetEventSlim gate)
		{

			if(!_client.Connected) return;
			const int timeoutMs = 2 * 1000;//Wait maximum 2 seconds
			var semaphoreWasSet = gate.Wait(timeoutMs, _mainCancellationToken);
			if(!semaphoreWasSet)
				_Logger.WarnFormat("Resetting connection [{0}]. Waited for semaphore to be released for {1} ms", _id, timeoutMs);
			else
				_Logger.DebugFormat("Resetting connection [{0}]", _id);
			Swallow(() =>
			{
				if(_client.Connected)
				{
					_client.Close();
					_Logger.DebugFormat("Closing connection [{0}]", _id);
				}

			}, ex => string.Format("Disconnecting {0} for connection {1} failed.", _hostPort, _id));
			EnsureConnectedAndGetStream();
		}




		public Task<byte[]> RequestAsync(IKafkaMessageWriteable message, int requestId)
		{
			return RequestAsync(message, requestId, CancellationToken.None);
		}

		public Task<byte[]> RequestAsync([NotNull] IKafkaMessageWriteable message, int requestId, CancellationToken cancellationToken)
		{
			if(message == null) throw new ArgumentNullException("message");
			//Check that user has called Connect() first
			if(!_userHasCalledConnectGate.IsSet)
				throw new InvalidOperationException("Connect() must be called first");	//Yes, it's ok to directly throw exceptions, in response to a usage error. See http://www.microsoft.com/en-us/download/details.aspx?id=19957

			var taskCompletionSource = new TaskCompletionSource<byte[]>();
			var mainCancellationToken = _mainCancellationToken;

			//Check if cancellation already have been requested
			if(mainCancellationToken.IsCancellationRequested)
			{
				return TaskHelper.FromCancellation<byte[]>(mainCancellationToken);
			}
			if(cancellationToken.IsCancellationRequested)
			{
				return TaskHelper.FromCancellation<byte[]>(mainCancellationToken);
			}


			var linkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(mainCancellationToken, cancellationToken);
			var messageContainer = new MessageContainer(message, requestId, taskCompletionSource, linkedTokenSource.Token);

			try
			{
				_sendBlockingCollection.Add(messageContainer, cancellationToken);
			}
			catch(OperationCanceledException)
			{
				taskCompletionSource.SetException(new OperationCanceledException("The message was not sent. The operation was canceled."));
				return taskCompletionSource.Task;
			}
			catch(InvalidOperationException)
			{
				taskCompletionSource.SetException(new OperationCanceledException("The message was not sent. The connection has shutdown."));
				return taskCompletionSource.Task;
			}
			return taskCompletionSource.Task;
		}


		// Receiving ---------------------------------------------------------------------------------------------------
		private void ProcessReceives(object state)
		{
			try
			{
				_Logger.InfoFormat("Started ProcessReceives on thread {0}", Thread.CurrentThread.ManagedThreadId);

				var numberOfConsecutiveReadErrors = 0;
				while(!_mainCancellationToken.IsCancellationRequested)
				{
					var networkStream = EnsureConnectedAndGetStream();

					//Start by reading first 4 bytes to get the size of the rest of the message
					var nullableSize = ReadSize(networkStream, ref numberOfConsecutiveReadErrors);
					if(!nullableSize.HasValue)
					{
						//An error occurred while reading the size. 
						//It could be that _mainCancellationToken was canceled, in which case the while loop will stop
						//In case of another error, the connection has been reset so we're good to go anouther round
						continue;
					}
					var size = nullableSize.Value;
					_Logger.DebugFormat("Received start of a message of {0} bytes", size);

					//Create buffer to read the rest of the stream
					var buffer = new byte[size];
					try
					{
						ReadFromStream(networkStream, buffer, 0, size, _mainCancellationToken);

						//Find which task to complete
						var correlationId = buffer.GetIntFromBigEndianBytes();
						MessageContainer messageContainer;
						if(_messagesByCorrelationId.TryRemove(correlationId, out messageContainer))
						{
							var requestId = messageContainer.RequestId;
							_Logger.DebugFormat("Received {2} bytes for correlation id={0}, request id={1}. Completing task.", correlationId, requestId, size);
							messageContainer.TaskCompletionSource.TrySetResult(buffer);
						}
						else
						{
							_Logger.ErrorFormat("Could not find a {1} for message with correlation id={0}. Dropping {2} bytes.", correlationId, typeof(MessageContainer));
						}
					}
					catch(OperationCanceledException)
					{
						_Logger.DebugFormat("ProcessReceives canceled");
						//This instance has shut down. Just continue in the loop, and we'll exit it.
						continue;
					}
					catch(ErrorReadingException e)
					{
						//how much did we read? If we read enough, we can extract the correlation id
						var bytesRead = e.BytesRead;
						if(bytesRead > BitConversion.IntSize)
						{
							var correlationId = buffer.GetIntFromBigEndianBytes();
							_Logger.ErrorException(e, "Error while receiving the message with correlation id={0}: {1}", correlationId, e.Message);
							MessageContainer messageContainer;
							if(_messagesByCorrelationId.TryRemove(correlationId, out messageContainer))
							{
								var requestId = messageContainer.RequestId;
								_Logger.DebugFormat("Canceling the task for correlation id={0}, request id={1}.", correlationId, requestId);
								messageContainer.TaskCompletionSource.TrySetException(new KafkaException(string.Format("Receiving request {0} failed: {1}", requestId, e.Message), e));
							}
							else
							{
								_Logger.ErrorFormat("Could not find a {1} for message with correlation id={0}.", correlationId, typeof(MessageContainer));
							}
						}
						numberOfConsecutiveReadErrors++;
					}
				}
				_Logger.InfoFormat("Stopped ProcessReceives on thread {0}", Thread.CurrentThread.ManagedThreadId);
			}
			catch(Exception e)
			{
				_Logger.FatalException(e, "ProcessReceives crashed. Unhandled exception. on thread {0}: {1}", Thread.CurrentThread.ManagedThreadId, e.Message);
			}
		}

		private int? ReadSize(NetworkStream networkStream, ref int numberOfConsecutiveSocketExceptions)
		{
			try
			{
				ReadFromStream(networkStream, _sizeBuffer, 0, BitConversion.IntSize, _mainCancellationToken);
				numberOfConsecutiveSocketExceptions = 0;
				var size = _sizeBuffer.GetIntFromBigEndianBytes();
				return size;
			}
			catch(OperationCanceledException)
			{
				_Logger.DebugFormat("ProcessReceives canceled");
				//This instance has shut down. Just continue in the loop, and we'll exit it.
			}
			catch(ErrorReadingException e)
			{
				_Logger.WarnFormat("An error occurred  while reading size (first {0} bytes) of the received message: {1}", BitConversion.IntSize, e.Message);

				//Something failed in ReadFromStream. 
				numberOfConsecutiveSocketExceptions++;

				const int maxNumberOfConsecutiveSocketExceptions = 10;
				if(numberOfConsecutiveSocketExceptions > maxNumberOfConsecutiveSocketExceptions)
				{
					_Logger.FatalException(e, "Received {0} SocketExceptions in a row. Terminating ProcessReceives. The last exception was: {1}", numberOfConsecutiveSocketExceptions, e.Message);
					_cancellationTokenSource.Cancel();
				}
			}
			return null;
		}

		private void ReadFromStream(NetworkStream networkStream, byte[] buffer, int startIndex, int length, CancellationToken cancellationToken)
		{
			var bytesRead = 0;
			Exception exception = null;
			try
			{
				while(bytesRead < length && networkStream.CanRead)
				{
					try
					{
						bytesRead += networkStream.ReadAsync(buffer, startIndex + bytesRead, length - bytesRead, cancellationToken).Result;
					}
					catch(SocketException e)
					{
						ResetConnection(_isSendingGate); //Block if is receiving
						throw new ErrorReadingException(bytesRead, string.Format("Did not receive everything. Expected {0} bytes. Only received {1} bytes on connection [{2}].", length, bytesRead, _id), e);
					}
				}
			}
			catch(OperationCanceledException)
			{
				_Logger.DebugFormat("The operation was canceled when receiving {0} bytes from Kafka {1} on connection [{2}].", length, _hostPort, _id);
				throw;
			}
			catch(Exception e)
			{
				exception = e;
			}
			if(bytesRead < length || exception != null)
			{
				_Logger.WarnException(exception, "Unable to receive {0} bytes from Kafka {1} on connection [{2}]. Only received {3} bytes. Resetting connection.", length, _hostPort, _id, bytesRead);
				try
				{
					ResetConnection(_isSendingGate);//Block if is receiving
				}
				catch(Exception e)
				{
					_Logger.WarnException(e, "Exception occurred while resetting connection: {0}" + e.Message);
				}
				throw new ErrorReadingException(bytesRead, string.Format("Did not receive everything. Expected {0} bytes. Only received {1} bytes on connection [{2}].", length, bytesRead, _id), exception);
			}
		}
		// Sending -----------------------------------------------------------------------------------------------------

		private void ProcessSends(object state)
		{
			try
			{
				var collection = _sendBlockingCollection;
				_Logger.InfoFormat("Started ProcessSends on thread {0}", Thread.CurrentThread.ManagedThreadId);
				while(!collection.IsCompleted && !_mainCancellationToken.IsCancellationRequested)
				{
					MessageContainer message = null;
					// Blocks if number.Count == 0 
					try
					{
						message = collection.Take();
					}
					catch(InvalidOperationException)
					{
						// InvalidOperationException means that Take() was called on a completed collection. 
						break;
					}

					if(message != null)
					{
						_Logger.Debug(h => h("Request {0}", message));
						HandleMessage(message);
					}
				}
				_Logger.InfoFormat("Stopped ProcessSends on thread {0}", Thread.CurrentThread.ManagedThreadId);
			}
			catch(Exception e)
			{
				_Logger.FatalException(e, "ProcessSends crashed. Unhandled exception. on thread {0}: {1}", Thread.CurrentThread.ManagedThreadId, e.Message);
			}

		}

		private void HandleMessage(MessageContainer message)
		{
			var requestId = message.RequestId;
			//Check if the user has requested us to cancel sending
			if(message.CancellationToken.IsCancellationRequested)
			{
				_Logger.DebugFormat("Request {0} was canceled", requestId);
				message.TaskCompletionSource.TrySetCanceled();
				return;
			}

			//Generate Correlation Id and store the message in internal dictionary, so that receive later on may pick it up.
			var correlationId = GetNextCorrelationId();
			_messagesByCorrelationId[correlationId] = message;

			//Send the message
			try
			{
				Send(message.MessageWriteable, requestId, correlationId, 1);
			}
			catch(Exception e)
			{
				if(e is OperationCanceledException)
					message.TaskCompletionSource.TrySetCanceled();
				else
					message.TaskCompletionSource.TrySetException(new SendFailedException(string.Format("Send request {3} to Kafka {0} on connection [{1}] failed. {2} See inner exception for details.", _hostPort, _id, e.Message, requestId)));

				//Remove the messagecontainer from internal dictionary, as we will not need to process any responses
				MessageContainer ignored;
				_messagesByCorrelationId.TryRemove(correlationId, out ignored);
			}
		}

		private void Send(IKafkaMessageWriteable writeable, int requestId, int correlationId, int retries)
		{
			var length = writeable.GetSize();
			_Logger.DebugFormat("About to send {2} bytes for request {0} correlation id={4} to Kafka {1} on connection [{3}]", requestId, _hostPort, length, _id, correlationId);
			while(true)
			{
				try
				{
					SendToServer(writeable, correlationId);
					return;
				}
				catch(OperationCanceledException e)
				{
					_Logger.DebugException(e, "Sending of request {0} correlation id={3} to Kafka {1} on connection [{2}] was canceled. Resetting connection.", requestId, _hostPort, _id, correlationId);
					//Reset the connection unless, the whole KafkaConnection has been canceled
					if(!_mainCancellationToken.IsCancellationRequested)
						ResetConnection(_isReceivingSemaphore); //If any ongoing receives is happening, then we'll block 
					throw;
				}
				catch(SocketException e)
				{
					if(retries > 0)
					{
						_Logger.WarnException(e, "Unable to send request {0} correlation id={3} to Kafka {1} on connection [{2}]. Retrying.", requestId, _hostPort, _id, correlationId);
						ResetConnection(_isReceivingSemaphore); //If any ongoing receives is happening, then we'll block 
						retries--;
					}
					else
					{
						_Logger.ErrorException(e, "Unable to send request {0} correlation id={3} to Kafka {1} on connection [{2}]. Failing.", requestId, _hostPort, _id, correlationId);
						ResetConnection(_isReceivingSemaphore); //If any ongoing receives is happening, then we'll block 
						throw;
					}
				}
			}
		}

		private void SendToServer(IKafkaMessageWriteable writeable, int correlationId)
		{
			try
			{
				_isSendingGate.Set();
				var networkStream = EnsureConnectedAndGetStream();
				var serializedSize = BitConversion.GetBigEndianBytes(writeable.GetSize());
				networkStream.Write(serializedSize, 0, BitConversion.IntSize);
				writeable.WriteTo(networkStream, correlationId);
			}
			finally
			{
				_isSendingGate.Reset();
			}
		}

		// Helpers -----------------------------------------------------------------------------------------------------
		private int GetNextCorrelationId()
		{
			return Interlocked.Increment(ref _correlationId);
		}

		private void Swallow(Action action, Func<Exception, string> createMessage)
		{
			_Logger.SwallowAsWarning(action, createMessage);
		}

		public override string ToString()
		{
			return _hostPort.ToString();
		}


		protected class MessageContainer
		{
			private readonly IKafkaMessageWriteable _messageWriteable;
			private readonly int _requestId;
			private readonly TaskCompletionSource<byte[]> _taskCompletionSource;
			private readonly CancellationToken _cancellationToken;

			public MessageContainer([NotNull] IKafkaMessageWriteable messageWriteable, int requestId, [NotNull] TaskCompletionSource<byte[]> taskCompletionSource, CancellationToken cancellationToken)
			{
				if(messageWriteable == null) throw new ArgumentNullException("messageWriteable");
				if(taskCompletionSource == null) throw new ArgumentNullException("taskCompletionSource");
				_messageWriteable = messageWriteable;
				_requestId = requestId;
				_taskCompletionSource = taskCompletionSource;
				_cancellationToken = cancellationToken;
			}

			public IKafkaMessageWriteable MessageWriteable { get { return _messageWriteable; } }

			public TaskCompletionSource<byte[]> TaskCompletionSource { get { return _taskCompletionSource; } }

			public CancellationToken CancellationToken { get { return _cancellationToken; } }

			public int RequestId { get { return _requestId; } }

			public override string ToString()
			{
				var sb = new StringBuilder();
				sb.Append("Request ").Append(_requestId).Append(", <").Append(_messageWriteable).Append('>');
				if(_cancellationToken.IsCancellationRequested)
					sb.Append(" CANCELED");
				return sb.ToString();
			}
		}
	}

}