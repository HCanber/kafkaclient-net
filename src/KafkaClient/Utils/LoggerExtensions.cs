using System;
using Common.Logging;

namespace KafkaClient.Utils
{
	public static class LoggerExtensions
	{
		public static void SwallowAsWarning(this ILog log, Action action)
		{
			Swallow(action,log.Warn);
		}

		private static void Swallow(Action action, Action<string,Exception> log)
		{
			try
			{
				action();
			}
			catch(Exception ex)
			{
				log(ex.Message, ex);
			}
		}
	}
}