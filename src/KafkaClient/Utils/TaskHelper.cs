using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Client.Utils
{
	public static class TaskHelper
	{
		//Some code comes from https://github.com/slashdotdash/ParallelExtensionsExtras, http://blogs.msdn.com/b/pfxteam/archive/2010/04/04/9990342.aspx


		public static Task AsTask(this CancellationToken cancellationToken)
		{
			var tcs = new TaskCompletionSource<object>();
			cancellationToken.Register(() => tcs.TrySetCanceled(),useSynchronizationContext: false);
			return tcs.Task;
		}

		/// <summary>Gets the TaskScheduler instance that should be used to schedule tasks.</summary>
		public static TaskScheduler GetTargetScheduler(this TaskFactory factory, TaskScheduler defaultTaskScheduler=null)
		{
			if(factory == null) throw new ArgumentNullException("factory");
			return factory.Scheduler ?? defaultTaskScheduler ?? TaskScheduler.Current;
		}


		/// <summary>Creates a Task that has completed in the Faulted state with the specified exception.</summary>
		/// <param name="factory">The target TaskFactory.</param>
		/// <param name="exception">The exception with which the Task should fault.</param>
		/// <returns>The completed Task.</returns>
		public static Task FromException(this TaskFactory factory, Exception exception)
		{
			var tcs = new TaskCompletionSource<object>(factory.CreationOptions);
			tcs.SetException(exception);
			return tcs.Task;
		}

		/// <summary>Creates a Task that has completed in the Faulted state with the specified exception.</summary>
		/// <typeparam name="TResult">Specifies the type of payload for the new Task.</typeparam>
		/// <param name="factory">The target TaskFactory.</param>
		/// <param name="exception">The exception with which the Task should fault.</param>
		/// <returns>The completed Task.</returns>
		public static Task<TResult> FromException<TResult>(this TaskFactory factory, Exception exception)
		{
			var tcs = new TaskCompletionSource<TResult>(factory.CreationOptions);
			tcs.SetException(exception);
			return tcs.Task;
		}

		/// <summary>Creates a Task that has completed in the RanToCompletion state with the specified result.</summary>
		/// <typeparam name="TResult">Specifies the type of payload for the new Task.</typeparam>
		/// <param name="factory">The target TaskFactory.</param>
		/// <param name="result">The result with which the Task should complete.</param>
		/// <returns>The completed Task.</returns>
		public static Task<TResult> FromResult<TResult>(this TaskFactory factory, TResult result)
		{
			var tcs = new TaskCompletionSource<TResult>(factory.CreationOptions);
			tcs.SetResult(result);
			return tcs.Task;
		}

		/// <summary>Creates a Task that has completed in the Canceled state with the specified CancellationToken.</summary>
		/// <typeparam name="TResult">Specifies the type of payload for the new Task.</typeparam>
		/// <param name="cancellationToken">The CancellationToken with which the Task should complete.</param>
		/// <returns>The completed Task.</returns>
		/// <exception cref="InvalidOperationException">Thrown if <paramref name="cancellationToken"/> is not canceled.</exception>
		public static Task<TResult> FromCancellation<TResult>(CancellationToken cancellationToken)
		{
			if(!cancellationToken.IsCancellationRequested) throw new InvalidOperationException("The CancellationToken must be canceled");
			return new Task<TResult>(DelegateCache<TResult>.DefaultResult, cancellationToken);
		}
		/// <summary>Creates a Task that has completed in the Canceled state with the specified CancellationToken.</summary>
		/// <param name="cancellationToken">The CancellationToken with which the Task should complete.</param>
		/// <returns>The completed Task.</returns>
		/// <exception cref="InvalidOperationException">Thrown if <paramref name="cancellationToken"/> is not canceled.</exception>
		public static Task FromCancellation(CancellationToken cancellationToken)
		{
			if(!cancellationToken.IsCancellationRequested) throw new InvalidOperationException("The CancellationToken must be canceled");
			return new Task(() => { }, cancellationToken);
		}

		/// <summary>A cache of delegates.</summary>
		/// <typeparam name="TResult">The result type.</typeparam>
		private class DelegateCache<TResult>
		{
			/// <summary>Function that returns default(TResult).</summary>
			internal static readonly Func<TResult> DefaultResult = () => default(TResult);
		}
	}
}