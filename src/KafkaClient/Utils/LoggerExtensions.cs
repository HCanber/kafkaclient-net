using System;
using Common.Logging;

namespace Kafka.Client.Utils
{
	public static class LoggerExtensions
	{
		public static void InfoException(this ILog log, Exception ex, string format, params object[] args)
		{
			log.Info(handler => handler(format, args), ex);
		}

		public static void TraceException(this ILog log, Exception ex, string format, params object[] args)
		{
			log.Trace(handler => handler(format, args), ex);
		}

		public static void DebugException(this ILog log, Exception ex, string format, params object[] args)
		{
			log.Debug(handler => handler(format, args), ex);
		}

		public static void WarnException(this ILog log, Exception ex, string format, params object[] args)
		{
			log.Warn(handler => handler(format, args), ex);
		}

		public static void ErrorException(this ILog log, Exception ex, string format, params object[] args)
		{
			log.Error(handler => handler(format, args), ex);
		}

		public static void FatalException(this ILog log, Exception ex, string format, params object[] args)
		{
			log.Fatal(handler => handler(format, args), ex);
		}

		public static void SwallowAsWarning(this ILog log, Action action)
		{
			Swallow(action, ex=>log.WarnException(ex,ex.ToString()));
		}

		public static void SwallowAsWarning(this ILog log, Action action, Func<Exception, string> message)
		{
			Swallow(action, ex=>log.WarnException(ex,message(ex)));
		}

		private static void Swallow(Action action, Action<Exception> log)
		{
			try
			{
				action();
			}
			catch(Exception ex)
			{
				log(ex);
			}
		}
	}
}